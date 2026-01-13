package queue

import (
	"context"
	"errors"

	"github.com/jdpolicano/go-search/internal/store"
)

var ErrorFrontierEmpty = errors.New("frontier queue is empty")

type Queue[T any] interface {
	Enqueue(item ...T) error
	Dequeue() (T, error)
	Len() (int, error)
	Close() error
}

type SqlFrontierQueue struct {
	ctx     context.Context
	s       store.Store
	buffer  []store.FrontierItem
	bufSize int
}

func NewSqlQueue(ctx context.Context, s store.Store, bufSize int, seeds []string) (*SqlFrontierQueue, error) {
	if len(seeds) == 0 {
		return nil, errors.New("seeds cannot be empty")
	}

	if len(seeds) > bufSize {
		return nil, errors.New("number of seeds cannot exceed buffer size")
	}

	buffer := make([]store.FrontierItem, 0, bufSize)
	return &SqlFrontierQueue{ctx, s, buffer, bufSize}, nil
}

func (q *SqlFrontierQueue) Enqueue(items ...store.FrontierItem) error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	items, err = store.InsertFIBatch(q.ctx, conn, items)
	if err != nil {
		return err
	}

	if len(q.buffer) < q.bufSize {
		// space left in buffer
		spaceLeft := q.bufSize - len(q.buffer)
		// only add up to space left
		end := min(len(items), spaceLeft)
		// add items to buffer
		q.buffer = append(q.buffer, items[:end]...)
	}

	return nil
}

func (q *SqlFrontierQueue) Dequeue() (store.FrontierItem, error) {
	if len(q.buffer) == 0 {
		if err := q.refill(); err != nil {
			return store.FrontierItem{}, err
		}
	}

	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return store.FrontierItem{}, err
	}
	defer conn.Release()

	item := q.buffer[0]
	item.Status = store.StatusInProgress
	e := store.UpdateFIStatus(q.ctx, conn, item.UrlNorm, item.Status)
	q.buffer = q.buffer[1:]
	return item, e
}

func (q *SqlFrontierQueue) Len() (int, error) {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	count, err := store.GetFICountByStatus(q.ctx, conn, store.StatusUnvisited)
	if err != nil {
		return 0, err
	}
	return count + len(q.buffer), nil
}

func (q *SqlFrontierQueue) Close() error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return store.CleanupFrontier(q.ctx, conn)
}

// safety: refill buffer if empty, we only call this internally here so its safe to make that assumption
func (q *SqlFrontierQueue) refill() error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}

	rows, err := store.GetFIByStatusDepthSorted(q.ctx, conn, store.StatusUnvisited, q.bufSize)
	if err != nil {
		return err
	}

	defer rows.Close()
	items := make([]store.FrontierItem, 0, q.bufSize)
	for rows.Next() {
		var fi store.FrontierItem
		if err := fi.FromRows(rows); err != nil {
			return err
		}
		items = append(items, fi)
	}

	if len(items) == 0 {
		return ErrorFrontierEmpty
	}

	q.buffer = append(q.buffer, items...)
	return nil
}
