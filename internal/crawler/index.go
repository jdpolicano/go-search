package crawler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/store"
)

type IndexMessage struct {
	fi     store.FrontierItem
	ctx    context.Context
	cancel context.CancelFunc
	words  []string
}

type Index struct {
	s         *store.Store
	queue     *CrawlQueue
	crawler   *Crawler
	processor *Processor
	in        chan IndexMessage
	wg        *sync.WaitGroup
}

func NewIndex(ctx context.Context, s *store.Store, queue *CrawlQueue, crawler *Crawler, processor *Processor, wg *sync.WaitGroup) (*Index, error) {
	in := processor.index
	return &Index{s, queue, crawler, processor, in, wg}, nil
}

func (idx *Index) Run() {
	idx.startWorkflow()
	idx.firstPassage()
	fmt.Println("run finished")
}

func (idx *Index) firstPassage() {
	docStore := idx.s.IntoDocumentStore()
	postingStore := idx.s.IntoPostingStore()
	termStore := idx.s.IntoTermStore()
	for {
		im, ok := <-idx.in
		if !ok {
			fmt.Println("Index \"in\" channel closed, returning")
			break
		}
		// insert the document into the store and abort if there was an error.
		docId, err := docStore.InsertDoc(im.fi.Url, len(im.words))
		if err != nil {
			fmt.Printf("Error inserting document for %s: %s\n", im.fi.Url, err)
			continue
		}
		// insert the unique terms in the store and get a "term stat" obj.
		// The term stat obj is a record of the ids for each word and the frequency of each word
		// that was inserted.
		termStats, err := termStore.InsertTermsIncDf(im.words)
		if err != nil {
			fmt.Printf("Error inserting terms for document for %s: %s\n", im.fi.Url, err)
			continue
		}

		// finally, using the term ids, term frequencies, and the doc id, add the postings for this document
		if err := postingStore.InsertPostingMany(termStats.IntoPostings(docId)); err != nil {
			fmt.Printf("Error inserting postings for document for %s: %s\n", im.fi.Url, err)
			continue
		}
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
