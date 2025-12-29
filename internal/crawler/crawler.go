package crawler

import (
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/store"
)

type Crawler struct {
	s   *store.Store
	in  chan store.FrontierItem
	out chan ProcessorMessage
	wg  *sync.WaitGroup
}

func NewCrawler(s *store.Store, in chan store.FrontierItem, wg *sync.WaitGroup) *Crawler {
	out := make(chan ProcessorMessage)
	return &Crawler{s, in, out, wg}
}

func (c *Crawler) Run() {
	defer c.Close()
	for {
		item, ok := <-c.in
		if !ok {
			fmt.Println("Crawler \"in\" channel closed, returning")
			return
		}
		fmt.Println("Crawler handling url: ", item.Url)
		ur := NewUrlResource(item.Url)
		ioReader, ioErr := ur.GetReader()
		if ioErr != nil {
			c.handleIoError(item, ioErr)
			continue
		}
		c.out <- ProcessorMessage{item, ioReader}
	}
}

func (c *Crawler) handleIoError(item store.FrontierItem, err error) {
	fmt.Printf("Error getting reader for %s\n", item.Url)
	e := c.s.IntoFrontierStore().UpdateStatus(item.UrlNorm, store.StatusFailed)
	if e != nil {
		fmt.Printf("Error updating status to failed for %s: %s\n", item.UrlNorm, e)
	}
}

func (c *Crawler) Close() {
	fmt.Println("Closing crawler")
	close(c.out)
	c.wg.Done()
}
