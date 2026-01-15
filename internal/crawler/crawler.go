// Package crawler implements the web crawling functionality for the go-search engine.
// It handles fetching web content and coordinating with the processing pipeline.
package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/store"
)

// CrawlerMessage represents a message containing a frontier item to be crawled.
type CrawlerMessage struct {
	fi store.FrontierItem
}

// Crawler handles fetching web content from URLs and passing it to the processor.
// It manages HTTP requests and coordinates with the processing pipeline.
type Crawler struct {
	in     chan CrawlerMessage   // Input channel for crawl requests
	out    chan ProcessorMessage // Output channel for fetched content
	wg     *sync.WaitGroup       // WaitGroup for goroutine management
	s      store.Store           // Database store for status updates
	ctx    context.Context       // Context for cancellation
	cancel context.CancelFunc    // Cancel function for stopping the crawler
}

// NewCrawler creates a new Crawler instance with the given configuration.
func NewCrawler(ctx context.Context, cancel context.CancelFunc, s store.Store, in chan CrawlerMessage, wg *sync.WaitGroup) *Crawler {
	out := make(chan ProcessorMessage)
	return &Crawler{in, out, wg, s, ctx, cancel}
}

// Run starts the crawler's main loop, processing URLs from the input channel.
// It fetches web content and sends it to the processor for further handling.
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
			ioReader, ioErr := getReaderFromUrl(cm.fi.Url)
			if ioErr != nil {
				c.handleIoError(cm, ioErr)
				continue
			}

			c.out <- ProcessorMessage{cm.fi, ioReader}
		}
	}
}

// handleIoError handles I/O errors that occur during URL fetching.
func (c *Crawler) handleIoError(cm CrawlerMessage, err error) {
	fmt.Printf("Error getting reader for %s\n", cm.fi.Url)
	c.updateItemStatus(cm.fi.UrlNorm, store.StatusFailed)
}

// Close gracefully shuts down the crawler by closing channels and signaling completion.
func (c *Crawler) Close() {
	fmt.Println("Closing crawler")
	close(c.out)
	c.wg.Done()
}

// updates the status of a frontier item in the database.
func (c *Crawler) updateItemStatus(urlNorm string, status store.FrontierStatusEnum) error {
	conn, err := c.s.Pool.Acquire(c.ctx)
	if err != nil {
		fmt.Printf("Error acquiring connection to update status for %s: %s\n", urlNorm, err)
		return err
	}
	defer conn.Release()
	err = store.UpdateFIStatus(c.ctx, conn, urlNorm, status)
	if err != nil {
		fmt.Printf("Error updating status for %s: %s\n", urlNorm, err)
		return err
	}
	return nil
}
