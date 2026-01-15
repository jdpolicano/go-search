// Package crawler contains content processing functionality for the search engine.
package crawler

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/jdpolicano/go-search/internal/extract"
	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/store"
)

// ProcessorMessage represents a message containing fetched web content to be processed.
type ProcessorMessage struct {
	fi     store.FrontierItem // Frontier item metadata
	reader io.Reader          // Fetched content reader
}

// Processor handles the extraction and processing of web content.
// It parses HTML, extracts links and text, and coordinates with the queue and index.
type Processor struct {
	in     chan ProcessorMessage     // Input channel for pages from crawler
	queue  chan []store.FrontierItem // Output channel for new URLs to queue
	index  chan IndexMessage         // Output channel for processed content to index
	wg     *sync.WaitGroup           // WaitGroup for goroutine management
	parser *extract.HtmlParser       // HTML parser for content extraction
	s      store.Store               // Database store
	ctx    context.Context           // Context for cancellation
	cancel context.CancelFunc        // Cancel function for stopping the processor
}

// NewProcessor creates a new Processor instance with the given configuration.
func NewProcessor(ctx context.Context, cancel context.CancelFunc, s store.Store, in chan ProcessorMessage, queue chan []store.FrontierItem, langs []language.Language, wg *sync.WaitGroup) *Processor {
	index := make(chan IndexMessage)
	parser := extract.NewHtmlParser(langs)
	return &Processor{in, queue, index, wg, parser, s, ctx, cancel}
}

// Run starts the processor's main loop, handling incoming content from the crawler.
func (p *Processor) Run() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			fmt.Println("processor work canceled, quitting")
			return
		case pc, ok := <-p.in:
			if !ok {
				fmt.Println("Processor \"in\" channel closed")
				p.cancel()
				return
			}
			p.processMessage(pc)
		}
	}
}

// processMessage handles a single processor message by parsing HTML and coordinating outputs.
func (p *Processor) processMessage(pm ProcessorMessage) {
	// Parse the HTML content
	doc, parseErr := p.parser.Parse(pm.reader)
	if parseErr != nil {
		p.handleError(pm, parseErr)
		return
	}

	// Extract text, links, and metadata from the parsed document
	extracted, err := extract.ProcessHtmlDocument(doc)
	if err != nil {
		p.handleError(pm, err)
		return
	}

	// Send extracted content to both index and queue concurrently
	var wg sync.WaitGroup
	wg.Add(2)
	// send to index
	go p.sendToIndex(pm, extracted, &wg)
	// send to queue
	go p.sendToQueue(pm, extracted, &wg)
	// wait for both to be accepted before moving on.
	wg.Wait()
}

// handleError processes errors that occur during content processing.
func (p *Processor) handleError(pm ProcessorMessage, err error) {
	fmt.Printf("%s: %s\n", pm.fi.Url, err)
	conn, e := p.s.Pool.Acquire(p.ctx)
	if e != nil {
		fmt.Printf("Error acquiring connection to update status for %s: %s\n", pm.fi.UrlNorm, e)
		return
	}
	defer conn.Release()
	e = store.UpdateFIStatus(p.ctx, conn, pm.fi.UrlNorm, store.StatusFailed)
	if e != nil {
		fmt.Printf("Error updating status to failed for %s: %s\n", pm.fi.UrlNorm, e)
	}
}

// getIndexEntry creates an index entry from processed content.
func (p *Processor) getIndexEntry(pm ProcessorMessage, extracted extract.Extracted) (store.IndexEntry, error) {
	url := pm.fi.Url
	hash := extracted.Hash
	len := extracted.Len
	termFreqs := extracted.TermFreqs
	return store.NewIndexEntry(url, hash, len, termFreqs)
}

// getFrontierMessages creates frontier items from extracted links for queue processing.
func (p *Processor) getFrontierMessages(pc ProcessorMessage, links []string) []store.FrontierItem {
	items := make([]store.FrontierItem, 0, len(links))
	for _, link := range links {
		item, err := store.NewFrontierItemFromParent(pc.fi, link)
		if err != nil {
			fmt.Println(err)
			continue
		}
		items = append(items, item)
	}

	return items
}

// sendToIndex sends processed content to the index for storage.
func (p *Processor) sendToIndex(pm ProcessorMessage, extracted extract.Extracted, wg *sync.WaitGroup) error {
	entry, err := p.getIndexEntry(pm, extracted)
	if err != nil {
		return err
	}
	msg := IndexMessage{entry: entry}
	select {
	case <-p.ctx.Done():
		fmt.Println("Processor context done, not sending to index")
	case p.index <- msg:
		fmt.Printf("Processor sent %s to index\n", pm.fi.Url)
	}
	wg.Done()
	return nil
}

// sendToQueue sends extracted links to the queue for future crawling.
func (p *Processor) sendToQueue(pm ProcessorMessage, ex extract.Extracted, wg *sync.WaitGroup) {
	msgs := p.getFrontierMessages(pm, ex.Links)
	select {
	case <-p.ctx.Done():
		fmt.Println("Processor context done, not sending to queue")
	case p.queue <- msgs:
		fmt.Printf("Processor sent %d new urls to queue from %s\n", len(msgs), pm.fi.Url)
	}
	wg.Done()
}

// Close gracefully shuts down the processor by closing channels and signaling completion.
func (p *Processor) Close() {
	fmt.Println("Closing Processor")
	close(p.queue)
	close(p.index)
	p.wg.Done()
}
