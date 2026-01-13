package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/queue"
	"github.com/jdpolicano/go-search/internal/store"
)

type IndexMessage struct {
	entry store.IndexEntry
}

type Index struct {
	queue     *CrawlQueue
	crawler   *Crawler
	processor *Processor
	in        chan IndexMessage
	wg        *sync.WaitGroup
	s         store.Store
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewIndex(ctx context.Context, cancel context.CancelFunc, s store.Store, seeds []string, langs []language.Language, wg *sync.WaitGroup) (*Index, error) {
	sqlQueue, err := queue.NewSqlQueue(ctx, s, 500, seeds)
	if err != nil {
		return nil, err
	}

	for _, seed := range seeds {
		fi, err := store.NewFrontierItemFromSeed(seed)
		if err == nil {
			sqlQueue.Enqueue(fi)
		} else {
			fmt.Printf("Error creating frontier item from seed %s: %s\n", seed, err)
		}
	}
	queue := NewCrawlQueue(ctx, cancel, sqlQueue, wg)
	crawler := NewCrawler(ctx, cancel, s, queue.out, wg)
	processor := NewProcessor(ctx, cancel, s, crawler.out, queue.in, langs, wg)
	in := processor.index
	return &Index{queue, crawler, processor, in, wg, s, ctx, cancel}, nil
}

func (idx *Index) Run() {
	idx.startWorkflow()
	idx.firstPassage()
	fmt.Println("run finished")
}

func (idx *Index) firstPassage() {
	defer idx.wg.Done()
	for {
		select {
		case <-idx.ctx.Done():
			fmt.Println("Index work canceled, returning")
			return
		case im, ok := <-idx.in:
			if !ok {
				fmt.Println("Index \"in\" channel closed, returning")
				idx.cancel() // cancel the whole workflow if it hasn't already.
				return
			}

			tx, err := idx.s.Pool.Begin(idx.ctx)
			if err != nil {
				idx.handleError(im, err)
				continue
			}

			err = store.IndexDocumentInit(idx.ctx, tx, im.entry)
			if err != nil {
				tx.Rollback(idx.ctx)
				idx.handleError(im, err)
				continue
			}

			err = store.UpdateFIStatus(idx.ctx, tx, im.entry.UrlNorm, store.StatusCompleted)
			if err != nil {
				tx.Rollback(idx.ctx)
				idx.handleError(im, err)
				continue
			}

			err = tx.Commit(idx.ctx)
			if err != nil {
				idx.handleError(im, err)
				continue
			}

			fmt.Printf("Indexed document %s successfully\n", im.entry.Url)
		}
	}
}

func (idx *Index) handleError(im IndexMessage, err error) {
	fmt.Printf("Error indexing %s: %s\n", im.entry.Url, err)
	conn, e := idx.s.Pool.Acquire(idx.ctx)
	if e != nil {
		fmt.Printf("Error acquiring connection to update status for %s: %s\n", im.entry.Url, e)
		return
	}
	defer conn.Release()
	e = store.UpdateFIStatus(idx.ctx, conn, im.entry.UrlNorm, store.StatusFailed)
	if e != nil {
		fmt.Printf("Error updating status to failed for %s: %s\n", im.entry.UrlNorm, e)
	}
}

func (idx *Index) startWorkflow() {
	go idx.queue.Run()
	idx.wg.Add(1)
	go idx.crawler.Run()
	idx.wg.Add(1)
	go idx.processor.Run()
	idx.wg.Add(1)
}

func (idx *Index) Close() {
	fmt.Println("Closing main Index process")
	idx.queue.Close() // this should cascade through the pipeline.
	idx.crawler.Close()
	idx.processor.Close()
	idx.wg.Done()
}
