// Package crawler contains URL queue management for the web crawler.
package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/queue"
	"github.com/jdpolicano/go-search/internal/store"
)

// CrawlQueue manages the URL frontier for the web crawler.
// It handles enqueuing new URLs and dequeuing URLs for crawling in a breadth-first manner.
type CrawlQueue struct {
	queue  queue.Queue[store.FrontierItem] // Underlying queue implementation
	in     chan []store.FrontierItem       // Input channel for new URLs (BFS queue)
	out    chan CrawlerMessage             // Output channel for URLs to crawl
	wg     *sync.WaitGroup                 // WaitGroup for goroutine management
	ctx    context.Context                 // Context for cancellation
	cancel context.CancelFunc              // Cancel function for stopping the queue
}

// NewCrawlQueue creates a new CrawlQueue instance with the given configuration.
func NewCrawlQueue(ctx context.Context, cancel context.CancelFunc, q queue.Queue[store.FrontierItem], wg *sync.WaitGroup) *CrawlQueue {
	in, out := make(chan []store.FrontierItem), make(chan CrawlerMessage)
	return &CrawlQueue{q, in, out, wg, ctx, cancel}
}

// Run starts the crawl queue's main loop, managing URL dequeuing and enqueuing.
func (cq *CrawlQueue) Run() {
	defer cq.wg.Done()
	if l, err := cq.queue.Len(); err != nil || l == 0 {
		return
	}

	for {
		activeOut, top, err := cq.prepareNextMessage()

		if err != nil {
			break
		}

		select {
		case <-cq.ctx.Done():
			fmt.Println("CrawlQueue work canceled, returning")
			return
		case activeOut <- top:
			cq.handleOutgoingMessage(top)
		case items, ok := <-cq.in:
			if !ok {
				cq.handleInputChannelClosed()
				cq.cancel()
				return
			}
			cq.enqueueItems(items)
		}
	}
}

// prepareNextMessage prepares the next URL to be sent to the crawler.
func (cq *CrawlQueue) prepareNextMessage() (chan CrawlerMessage, CrawlerMessage, error) {
	item, err := cq.queue.Dequeue()
	if err == queue.ErrorFrontierEmpty {
		return nil, CrawlerMessage{}, nil
	} else if err != nil {
		fmt.Printf("Error dequeueing url: %s\n", err)
		return nil, CrawlerMessage{}, err
	}

	return cq.out, CrawlerMessage{
		fi: item,
	}, nil
}

// handleOutgoingMessage handles logging for outgoing messages to the crawler.
func (cq *CrawlQueue) handleOutgoingMessage(top CrawlerMessage) {
	fmt.Printf("Starting %s\n", top.fi.Url)
}

// handleInputChannelClosed handles the case when the input channel is closed.
func (cq *CrawlQueue) handleInputChannelClosed() {
	fmt.Println("Queue input channel closed")
	l, err := cq.queue.Len()
	if err != nil {
		fmt.Printf("Error getting length of queue: %s\n", err)
	} else {
		fmt.Printf("Final queue length: %d\n", l)
	}
}

// enqueueItems adds multiple frontier items to the queue, handling unique violations.
func (cq *CrawlQueue) enqueueItems(items []store.FrontierItem) {
	for _, item := range items {
		err := cq.queue.Enqueue(item)
		if err != nil {
			if !store.ErrorIsUniqueViolation(err) {
				fmt.Printf("Error enqueueing url %s: %s\n", item.Url, err)
			}
			continue
		}
	}
}

// Close gracefully shuts down the crawl queue by closing the underlying queue and channels.
func (cq *CrawlQueue) Close() {
	fmt.Println("Closing UrlQueue")
	if err := cq.queue.Close(); err != nil {
		fmt.Printf("Error closing queue: %s\n", err)
	}
	close(cq.out)
	cq.wg.Done()
}
