// Package queue provides queue management for the web crawler's URL frontier.
package queue

import (
	"context"
	"errors"

	"github.com/jdpolicano/go-search/internal/store"
)

// ErrorFrontierEmpty is returned when attempting to dequeue from an empty frontier queue.
var ErrorFrontierEmpty = errors.New("frontier queue is empty")

// Queue defines the interface for queue operations used by the crawler.
type Queue[T any] interface {
	Enqueue(item ...T) error // Add items to the queue
	Dequeue() (T, error)     // Remove and return the next item from the queue
	Len() (int, error)       // Get the current length of the queue
	Close() error            // Close the queue and cleanup resources
}

// SqlFrontierQueue implements a SQL-based queue for managing the crawler's URL frontier.
// It uses an in-memory buffer for performance and persists to the database.
type SqlFrontierQueue struct {
	ctx     context.Context      // Context for operations and cancellation
	s       store.Store          // Database store for persistence
	buffer  []store.FrontierItem // In-memory buffer for performance
	bufSize int                  // Maximum buffer size
}

// NewSqlQueue creates a new SQL-based frontier queue with the given configuration.
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

// Enqueue adds frontier items to the queue by persisting them to the database.
func (q *SqlFrontierQueue) Enqueue(items ...store.FrontierItem) error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = store.InsertFIBatch(q.ctx, conn, items)
	return err
}

// Dequeue removes and returns the next frontier item from the queue.
// It maintains an in-memory buffer for performance and refills from the database when empty.
func (q *SqlFrontierQueue) Dequeue() (store.FrontierItem, error) {
	if len(q.buffer) == 0 {
		if err := q.refill(); err != nil {
			return store.FrontierItem{}, err
		}
	}

	// Acquire a connection so we can mark the item as in-progress before returning it.
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return store.FrontierItem{}, err
	}
	defer conn.Release()

	item := q.buffer[0]
	q.buffer = q.buffer[1:]

	return item, nil
}

// Len returns the total length of the queue including both database and buffer items.
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

// Close cleans up the frontier by removing processed items and closing resources.
func (q *SqlFrontierQueue) Close() error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	return store.CleanupFrontier(q.ctx, conn)
}

// refill populates the buffer with unvisited frontier items from the database.
// Safety: This is only called internally, so we can safely assume the buffer is empty.
func (q *SqlFrontierQueue) refill() error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}
	// Ensure connection is released even if we return early
	defer conn.Release()

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

// insertSeeds converts seed URLs to frontier items and inserts them into the database.
func (q *SqlFrontierQueue) insertSeeds(seeds []string) error {
	conn, err := q.s.Pool.Acquire(q.ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	items := make([]store.FrontierItem, 0, len(seeds))
	for _, seed := range seeds {
		item, err := store.NewFrontierItemFromSeed(seed)
		if err != nil {
			return err
		}
		items = append(items, item)
	}
	_, err = store.InsertFIBatch(q.ctx, conn, items)
	return err
}
