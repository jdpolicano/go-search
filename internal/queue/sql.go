package queue

import (
	"errors"

	"github.com/jdpolicano/go-search/internal/store"
)

var ErrorFrontierEmpty = errors.New("frontier queue is empty")

type Queue[T any] interface {
	Enqueue(item T) error
	Dequeue() (T, error)
	Len() (int, error)
	Close() error
}

type SqlFrontierQueue struct {
	fs      *store.FrontierStore
	buffer  []store.FrontierItem
	bufSize int
}

func NewSqlQueue(s *store.Store, bufSize int, seeds []string) (*SqlFrontierQueue, error) {
	if len(seeds) == 0 {
		return nil, errors.New("seeds cannot be empty")
	}

	if len(seeds) > bufSize {
		return nil, errors.New("number of seeds cannot exceed buffer size")
	}

	fs := s.IntoFrontierStore()
	buffer := make([]store.FrontierItem, 0, bufSize)
	for _, seed := range seeds {
		fi, err := store.NewFrontierItemFromSeed(seed)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, fi)
	}
	err := fs.InsertMany(buffer)
	return &SqlFrontierQueue{fs, buffer, bufSize}, err
}

func (q *SqlFrontierQueue) Enqueue(item store.FrontierItem) error {
	err := q.fs.Insert(item)
	if err != nil {
		return err
	}
	if len(q.buffer) < q.bufSize {
		q.buffer = append(q.buffer, item)
	}
	return nil
}

func (q *SqlFrontierQueue) Dequeue() (store.FrontierItem, error) {
	if len(q.buffer) == 0 {
		items, err := q.fs.GetByStatusDepthSorted(store.StatusUnvisited, q.bufSize)
		if err != nil {
			return store.FrontierItem{}, err
		}
		if len(items) == 0 {
			return store.FrontierItem{}, ErrorFrontierEmpty
		}
		q.buffer = append(q.buffer, items...)
	}
	item := q.buffer[0]
	item.Status = store.StatusInProgress
	e := q.fs.UpdateStatus(item.UrlNorm, item.Status)
	q.buffer = q.buffer[1:]
	return item, e
}

func (q *SqlFrontierQueue) Len() (int, error) {
	count, err := q.fs.GetCountByStatus(store.StatusUnvisited)
	if err != nil {
		return 0, err
	}
	return count + len(q.buffer), nil
}

func (q *SqlFrontierQueue) Close() error {
	return q.fs.Cleanup()
}
