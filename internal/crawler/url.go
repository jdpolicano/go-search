package crawler

import (
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/queue"
	"github.com/jdpolicano/go-search/internal/store"
)

type CrawlQueue struct {
	queue queue.Queue[store.FrontierItem]
	in    chan []store.FrontierItem // data into the queue, for a bfs queue.
	out   chan store.FrontierItem   // send an item along the queue
	wg    *sync.WaitGroup
}

func NewCrawlQueue(s *store.Store, seeds []string, wg *sync.WaitGroup) (*CrawlQueue, error) {
	queue, err := queue.NewSqlQueue(s, 1024, seeds)
	if err != nil {
		return nil, err
	}
	in, out := make(chan []store.FrontierItem), make(chan store.FrontierItem)
	return &CrawlQueue{queue, in, out, wg}, nil
}

func (cq *CrawlQueue) Run() {
	if l, err := cq.queue.Len(); err != nil || l == 0 {
		return
	}

	for {
		var activeOut chan store.FrontierItem
		var top store.FrontierItem

		item, err := cq.queue.Dequeue()

		// if there are no items to dequeue, disable output channel
		if err == queue.ErrorFrontierEmpty {
			activeOut = nil
			// if there was another error, log and break
		} else if err != nil {
			fmt.Printf("Error dequeueing url: %s\n", err)
			break
			// otherwise, set the output channel and top item
		} else {
			activeOut = cq.out
			top = item
		}

		select {
		// a url is accepted by the downstream
		case activeOut <- top:
			{
				fmt.Printf("Starting %s\n", top.Url)
			}
		case items, ok := <-cq.in:
			{
				if !ok {
					fmt.Println("Queue input channel closed")
					l, e := cq.queue.Len()
					if e != nil {
						fmt.Printf("Error getting length of queue: %s\n", e)
						return
					} else {
						fmt.Printf("Final queue length: %d\n", l)
					}
					return
				}

				for _, item := range items {
					err := cq.queue.Enqueue(item)
					if err != nil {
						if !store.ErrorIsConstraintViolation(err) {
							fmt.Printf("Error enqueueing url %s: %s\n", item.Url, err)
							continue
						}
					}
				}
			}
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
