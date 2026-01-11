package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/queue"
	"github.com/jdpolicano/go-search/internal/store"
)

type CrawlQueue struct {
	queue   queue.Queue[store.FrontierItem]
	in      chan []store.FrontierItem // data into the queue, for a bfs queue.
	out     chan CrawlerMessage       // send an item along the queue
	wg      *sync.WaitGroup
	rootCtx context.Context
}

func NewCrawlQueue(ctx context.Context, q queue.Queue[store.FrontierItem], wg *sync.WaitGroup) *CrawlQueue {
	in, out := make(chan []store.FrontierItem), make(chan CrawlerMessage)
	return &CrawlQueue{q, in, out, wg, ctx}
}

func (cq *CrawlQueue) Run() {
	if l, err := cq.queue.Len(); err != nil || l == 0 {
		return
	}

	for {
		activeOut, top, err := cq.prepareNextMessage()

		if err != nil {
			break
		}

		select {
		case activeOut <- top:
			cq.handleOutgoingMessage(top)
		case items, ok := <-cq.in:
			if !ok {
				cq.handleInputChannelClosed()
				return
			}
			cq.enqueueItems(items)
		}
	}
}

func (cq *CrawlQueue) prepareNextMessage() (chan CrawlerMessage, CrawlerMessage, error) {
	item, err := cq.queue.Dequeue()
	if err == queue.ErrorFrontierEmpty {
		return nil, CrawlerMessage{}, nil
	} else if err != nil {
		fmt.Printf("Error dequeueing url: %s\n", err)
		return nil, CrawlerMessage{}, err
	}

	return cq.out, CrawlerMessage{
		fi:     item,
		ctx:    cq.rootCtx,
		cancel: nil,
	}, nil
}

func (cq *CrawlQueue) handleOutgoingMessage(top CrawlerMessage) {
	fmt.Printf("Starting %s\n", top.fi.Url)
}

func (cq *CrawlQueue) handleInputChannelClosed() {
	fmt.Println("Queue input channel closed")
	l, err := cq.queue.Len()
	if err != nil {
		fmt.Printf("Error getting length of queue: %s\n", err)
	} else {
		fmt.Printf("Final queue length: %d\n", l)
	}
}

func (cq *CrawlQueue) enqueueItems(items []store.FrontierItem) {
	for _, item := range items {
		err := cq.queue.Enqueue(item)
		if err != nil && !store.ErrorIsConstraintViolation(err) {
			fmt.Printf("Error enqueueing url %s: %s\n", item.Url, err)
		}
	}
}

func (cq *CrawlQueue) Close() {
	fmt.Println("Closing UrlQueue")
	if err := cq.queue.Close(); err != nil {
		fmt.Printf("Error closing queue: %s\n", err)
	}
	close(cq.out)
	cq.wg.Done()
}
