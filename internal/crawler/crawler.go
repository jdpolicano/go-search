package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/store"
)

type CrawlerMessage struct {
	fi     store.FrontierItem
	ctx    context.Context
	cancel context.CancelFunc
}

type Crawler struct {
	s   *store.Store
	in  chan CrawlerMessage
	out chan ProcessorMessage
	wg  *sync.WaitGroup
}

func NewCrawler(s *store.Store, in chan CrawlerMessage, wg *sync.WaitGroup) *Crawler {
	out := make(chan ProcessorMessage)
	return &Crawler{s, in, out, wg}
}

func (c *Crawler) Run() {
	for {
		cm, ok := <-c.in
		if !ok {
			fmt.Println("Crawler \"in\" channel closed, returning")
			return
		}
		fmt.Println("Crawler handling url: ", cm.fi.Url)
		ur := NewUrlResource(cm.fi.Url)
		ioReader, ioErr := ur.GetReader()
		if ioErr != nil {
			c.handleIoError(cm, ioErr)
			continue
		}
		c.out <- ProcessorMessage{cm.fi, cm.ctx, cm.cancel, ioReader}
	}
}

func (c *Crawler) handleIoError(cm CrawlerMessage, err error) {
	fmt.Printf("Error getting reader for %s\n", cm.fi.Url)
	e := c.s.IntoFrontierStore().UpdateStatus(cm.fi.UrlNorm, store.StatusFailed)
	if e != nil {
		fmt.Printf("Error updating status to failed for %s: %s\n", cm.fi.UrlNorm, e)
	}
}

func (c *Crawler) Close() {
	fmt.Println("Closing crawler")
	close(c.out)
	c.wg.Done()
}
