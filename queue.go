// queue.go - multi-writer multi-reader queue.
//
// To the extent possible under law, Ivan Markin waived all copyright
// and related or neighboring rights to rand, using the creative
// commons "cc0" public domain dedication. See LICENSE or
// <http://creativecommons.org/publicdomain/zero/1.0/> for full details.

package queue

import (
	"sync"
)

// Queue implements multiple-writer multiple-reader FIFO.
// To push a message into queue send it to channel C.
type Queue struct {
	C              chan interface{}
	salvager       func(interface{})
	receivers      map[*Receiver]bool
	receiversMutex sync.Mutex
}

// Create new Queue. Function salvager is being called on a message
// when all receivers have received it.
func New(salvager func(interface{})) *Queue {
	return NewWithBufferSize(salvager, 16)
}

// Like New() but the size of input buffer can be set via bufsize.
func NewWithBufferSize(salvager func(interface{}), bufsize int) *Queue {
	q := &Queue{
		C:         make(chan interface{}, bufsize),
		receivers: make(map[*Receiver]bool),
		salvager:  salvager,
	}
	go q.broadcaster()
	return q
}

// Internal worker to replicate received messages to the receivers.
func (q *Queue) broadcaster() {
	for v := range q.C {
		q.receiversMutex.Lock()
		var wg sync.WaitGroup
		for r := range q.receivers {
			wg.Add(1)
			go func(r *Receiver, v interface{}) {
				r.C <- v
				wg.Done()
			}(r, v)
		}
		q.receiversMutex.Unlock()
		go func(wg *sync.WaitGroup, v interface{}) {
			wg.Wait()
			q.salvager(v)
		}(&wg, v)
	}
}

// Receiver is an entity to receive messages from the queue.
// Messages will appear on channel C.
type Receiver struct {
	C chan interface{}
}

// Connect creates new Receiver attached to the queue.
// Only subsequent messages will appear on that Receiver.
func (q *Queue) Connect() *Receiver {
	return q.ConnectWithBufferSize(0)
}

// Like Connect but Receiver buffer size can be set via bufsize.
func (q *Queue) ConnectWithBufferSize(bufsize int) *Receiver {
	q.receiversMutex.Lock()
	defer q.receiversMutex.Unlock()
	r := &Receiver{
		C: make(chan interface{}, bufsize),
	}
	q.receivers[r] = true
	return r
}

// Disconnect detached specified Receiver from the queue so it
// will no longer receive messages. Note that C is closed after
// calling Disconnect.
func (q *Queue) Disconnect(r *Receiver) {
	q.receiversMutex.Lock()
	defer q.receiversMutex.Unlock()
	delete(q.receivers, r)
	close(r.C)
}
