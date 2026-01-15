// Package crawler contains the main indexing coordinator for the search engine.
package crawler

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/queue"
	"github.com/jdpolicano/go-search/internal/store"
)

// IndexMessage represents a message containing an index entry to be stored.
type IndexMessage struct {
	entry store.IndexEntry
}

// Index coordinates the entire crawling and indexing workflow.
// It manages the pipeline of queue -> crawler -> processor -> index.
type Index struct {
	queue     *CrawlQueue        // URL queue management
	crawler   *Crawler           // Web content fetching
	processor *Processor         // Content processing and extraction
	in        chan IndexMessage  // Input channel for index entries
	wg        *sync.WaitGroup    // WaitGroup for goroutine management
	s         store.Store        // Database store
	ctx       context.Context    // Context for cancellation
	cancel    context.CancelFunc // Cancel function for stopping the workflow
	logger    *slog.Logger       // Structured logger
}

// NewIndex creates a new Index instance with the given configuration.
// It sets up the entire crawling pipeline and initializes seed URLs.
func NewIndex(ctx context.Context, cancel context.CancelFunc, s store.Store, seeds []string, langs []language.Language, wg *sync.WaitGroup, logger *slog.Logger) (*Index, error) {
	// Create SQL-based queue with capacity of 500
	sqlQueue, err := queue.NewSqlQueue(ctx, s, 500, seeds)
	if err != nil {
		return nil, err
	}

	// Add seed URLs to the queue
	for _, seed := range seeds {
		fi, err := store.NewFrontierItemFromSeed(seed)
		if err == nil {
			sqlQueue.Enqueue(fi)
		} else {
			logger.Error("Error creating frontier item from seed", "seed", seed, "error", err)
		}
	}

	// Set up the crawling pipeline
	queue := NewCrawlQueue(ctx, cancel, sqlQueue, wg, logger)
	crawler := NewCrawler(ctx, cancel, s, queue.out, wg, logger)
	processor := NewProcessor(ctx, cancel, s, crawler.out, queue.in, langs, wg, logger)
	in := processor.index
	return &Index{queue, crawler, processor, in, wg, s, ctx, cancel, logger}, nil
}

// Run starts the indexing workflow by initializing all components and processing index entries.
func (idx *Index) Run() {
	idx.startWorkflow()
	idx.firstPassage()
	idx.logger.Info("Index run finished")
}

// firstPassage processes index entries from the input channel and stores them in the database.
// It handles transactions and updates frontier item status upon completion.
func (idx *Index) firstPassage() {
	defer idx.wg.Done()
	for {
		select {
		case <-idx.ctx.Done():
			idx.logger.Info("Index work canceled, returning")
			return
		case im, ok := <-idx.in:
			if !ok {
				idx.logger.Info("Index \"in\" channel closed, returning")
				idx.cancel() // cancel the whole workflow if it hasn't already.
				return
			}

			// Begin transaction for atomic database operations
			tx, err := idx.s.Pool.Begin(idx.ctx)
			if err != nil {
				idx.handleError(im, err)
				continue
			}

			// Index the document
			err = store.IndexDocumentInit(idx.ctx, tx, im.entry)
			if err != nil {
				tx.Rollback(idx.ctx)
				idx.handleError(im, err)
				continue
			}

			// Update frontier item status to completed
			err = store.UpdateFIStatus(idx.ctx, tx, im.entry.UrlNorm, store.StatusCompleted)
			if err != nil {
				tx.Rollback(idx.ctx)
				idx.handleError(im, err)
				continue
			}

			// Commit the transaction
			err = tx.Commit(idx.ctx)
			if err != nil {
				idx.handleError(im, err)
				continue
			}

			idx.logger.Info("Indexed document successfully", "url", im.entry.Url)
		}
	}
}

// handleError processes errors that occur during indexing by updating the frontier item status.
func (idx *Index) handleError(im IndexMessage, err error) {
	idx.logger.Error("Error indexing document", "url", im.entry.Url, "error", err)
	conn, e := idx.s.Pool.Acquire(idx.ctx)
	if e != nil {
		idx.logger.Error("Error acquiring connection to update status", "url", im.entry.Url, "error", e)
		return
	}
	defer conn.Release()
	e = store.UpdateFIStatus(idx.ctx, conn, im.entry.UrlNorm, store.StatusFailed)
	if e != nil {
		idx.logger.Error("Error updating status to failed", "url", im.entry.UrlNorm, "error", e)
	}
}

// startWorkflow initializes and starts all components of the crawling pipeline.
func (idx *Index) startWorkflow() {
	go idx.queue.Run()
	idx.wg.Add(1)
	go idx.crawler.Run()
	idx.wg.Add(1)
	go idx.processor.Run()
	idx.wg.Add(1)
}

// Close gracefully shuts down the index and all its components.
func (idx *Index) Close() {
	idx.logger.Info("Closing main Index process")
	idx.queue.Close() // this should cascade through the pipeline.
	idx.crawler.Close()
	idx.processor.Close()
	idx.wg.Done()
}
