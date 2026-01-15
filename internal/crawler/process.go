// Package crawler contains content processing functionality for the search engine.
package crawler

import (
	"context"
	"io"
	"log/slog"
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
	logger *slog.Logger              // Structured logger
}

// NewProcessor creates a new Processor instance with the given configuration.
func NewProcessor(ctx context.Context, cancel context.CancelFunc, s store.Store, in chan ProcessorMessage, queue chan []store.FrontierItem, langs []language.Language, wg *sync.WaitGroup, logger *slog.Logger) *Processor {
	index := make(chan IndexMessage)
	parser := extract.NewHtmlParser(langs)
	return &Processor{in, queue, index, wg, parser, s, ctx, cancel, logger}
}

// Run starts the processor's main loop, handling incoming content from the crawler.
func (p *Processor) Run() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("processor work canceled, quitting")
			return
		case pc, ok := <-p.in:
			if !ok {
				p.logger.Info("Processor \"in\" channel closed")
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
	p.logger.Error("Content processing error", "url", pm.fi.Url, "error", err)
	conn, e := p.s.Pool.Acquire(p.ctx)
	if e != nil {
		p.logger.Error("Error acquiring connection to update status", "url", pm.fi.UrlNorm, "error", e)
		return
	}
	defer conn.Release()
	e = store.UpdateFIStatus(p.ctx, conn, pm.fi.UrlNorm, store.StatusFailed)
	if e != nil {
		p.logger.Error("Error updating status to failed", "url", pm.fi.UrlNorm, "error", e)
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
			p.logger.Warn("Error creating frontier item from link", "url", pc.fi.Url, "link", link, "error", err)
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
		p.logger.Info("Processor context done, not sending to index")
	case p.index <- msg:
		p.logger.Info("Processor sent to index", "url", pm.fi.Url)
	}
	wg.Done()
	return nil
}

// sendToQueue sends extracted links to the queue for future crawling.
func (p *Processor) sendToQueue(pm ProcessorMessage, ex extract.Extracted, wg *sync.WaitGroup) {
	msgs := p.getFrontierMessages(pm, ex.Links)
	select {
	case <-p.ctx.Done():
		p.logger.Info("Processor context done, not sending to queue")
	case p.queue <- msgs:
		p.logger.Info("Processor sent new URLs to queue", "url", pm.fi.Url, "count", len(msgs))
	}
	wg.Done()
}

// Close gracefully shuts down the processor by closing channels and signaling completion.
func (p *Processor) Close() {
	p.logger.Info("Closing Processor")
	close(p.queue)
	close(p.index)
	p.wg.Done()
}
