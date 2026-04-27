package main

import (
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type queue struct {
	msgs []string
	wait []*waiter
}

var (
	mu     sync.Mutex
	queues = map[string]*queue{}
)

func getQueue(name string) *queue {
	q := queues[name]
	if q == nil {
		q = &queue{}
		queues[name] = q
	}
	return q
}

func handler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[1:]
	if name == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {

	case http.MethodPut:
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mu.Lock()
		q := getQueue(name)

		if len(q.wait) > 0 {
			x := q.wait[0]
			q.wait = q.wait[1:]
			mu.Unlock()
			x.ch <- v
		} else {
			q.msgs = append(q.msgs, v)
			mu.Unlock()
		}

		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		timeout, _ := strconv.Atoi(r.URL.Query().Get("timeout"))

		mu.Lock()
		q := getQueue(name)

		if len(q.msgs) > 0 {
			v := q.msgs[0]
			q.msgs = q.msgs[1:]
			mu.Unlock()
			w.Write([]byte(v))
			return
		}

		if timeout <= 0 {
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}

		x := &waiter{ch: make(chan string, 1)}
		q.wait = append(q.wait, x)
		mu.Unlock()

		select {
		case v := <-x.ch:
			w.Write([]byte(v))

		case <-time.After(time.Duration(timeout) * time.Second):
			mu.Lock()
			q := getQueue(name)
			for i, w := range q.wait {
				if w == x {
					q.wait = append(q.wait[:i], q.wait[i+1:]...)
					break
				}
			}
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: go run main.go <port>")
	}
	http.HandleFunc("/", handler)
	panic(http.ListenAndServe(":"+os.Args[1], nil))
}