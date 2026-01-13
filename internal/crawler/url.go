package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/queue"
	"github.com/jdpolicano/go-search/internal/store"
)

type CrawlQueue struct {
	queue  queue.Queue[store.FrontierItem]
	in     chan []store.FrontierItem // data into the queue, for a bfs queue.
	out    chan CrawlerMessage       // send an item along the queue
	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewCrawlQueue(ctx context.Context, cancel context.CancelFunc, q queue.Queue[store.FrontierItem], wg *sync.WaitGroup) *CrawlQueue {
	in, out := make(chan []store.FrontierItem, 100), make(chan CrawlerMessage, 100)
	return &CrawlQueue{q, in, out, wg, ctx, cancel}
}

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
		if err != nil {
			if !store.ErrorIsUniqueViolation(err) {
				fmt.Printf("Error enqueueing url %s: %s\n", item.Url, err)
			}
			continue
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
