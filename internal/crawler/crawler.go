package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/store"
)

type CrawlerMessage struct {
	fi store.FrontierItem
}

type Crawler struct {
	in     chan CrawlerMessage
	out    chan ProcessorMessage
	wg     *sync.WaitGroup
	s      store.Store
	ctx    context.Context
	cancel context.CancelFunc
}

func NewCrawler(ctx context.Context, cancel context.CancelFunc, s store.Store, in chan CrawlerMessage, wg *sync.WaitGroup) *Crawler {
	out := make(chan ProcessorMessage, 100)
	return &Crawler{in, out, wg, s, ctx, cancel}
}

func (c *Crawler) Run() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("Crawler work canceled, returning")
			return
		case cm, ok := <-c.in:
			if !ok {
				fmt.Println("Crawler \"in\" channel closed, returning")
				c.cancel()
				return
			}
			fmt.Println("Crawler handling url: ", cm.fi.Url)
			ioReader, ioErr := GetReaderFromUrl(cm.fi.Url)
			if ioErr != nil {
				c.handleIoError(cm, ioErr)
				continue
			}
			c.out <- ProcessorMessage{cm.fi, ioReader}
		}
	}
}

func (c *Crawler) handleIoError(cm CrawlerMessage, err error) {
	fmt.Printf("Error getting reader for %s\n", cm.fi.Url)
	conn, err := c.s.Pool.Acquire(c.ctx)
	if err != nil {
		fmt.Printf("Error acquiring connection to update status for %s: %s\n", cm.fi.UrlNorm, err)
		return
	}
	defer conn.Release()
	e := store.UpdateFIStatus(c.ctx, conn, cm.fi.UrlNorm, store.StatusFailed)
	if e != nil {
		fmt.Printf("Error updating status to failed for %s: %s\n", cm.fi.UrlNorm, e)
	}
}

func (c *Crawler) Close() {
	fmt.Println("Closing crawler")
	close(c.out)
	c.wg.Done()
}
