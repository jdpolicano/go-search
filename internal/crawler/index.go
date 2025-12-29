package crawler

import (
	"fmt"
	"sync"

	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/store"
)

type IndexMessage struct {
	item  store.FrontierItem
	words []string
}

type Index struct {
	s         *store.Store
	queue     *CrawlQueue
	crawler   *Crawler
	processor *Processor
	in        chan IndexMessage
	wg        *sync.WaitGroup
}

func NewIndex(s *store.Store, seeds []string, langs []language.Language, wg *sync.WaitGroup) (*Index, error) {
	queue, err := NewCrawlQueue(s, seeds, wg)
	if err != nil {
		fmt.Printf("Error creating CrawlQueue: %s\n", err)
		return nil, err
	}
	crawler := NewCrawler(s, queue.out, wg)
	processor := NewProcessor(s, crawler.out, queue.in, langs, wg)
	in := processor.index
	return &Index{s, queue, crawler, processor, in, wg}, nil
}

func (idx *Index) Run() {
	idx.startWorkflow()

	for {
		// im, ok := <-processor.index
		// if !ok {
		// 	fmt.Println("Index \"in\" channel closed, returning")
		// 	break
		// }

	}
}

func (idx *Index) firstPassage(im IndexMessage) {
	docStore := idx.s.IntoDocumentStore()
	postingStore := idx.s.IntoPostingStore()
	for {
		im, ok := <-idx.in
		if !ok {
			fmt.Println("Index \"in\" channel closed, returning")
			break
		}

		doc := store.NewDoc(im.item.Url, len(im.words))
		docId, err := docStore.Insert(doc)
		if err != nil {
			fmt.Printf("Error inserting document for %s: %s\n", im.item.Url, err)
			continue
		}
		doc.ID = docId
		// we get the unique terms first so we can update our postings table correctly
		uniquePostings, err := idx.insertTerms(docId, im.words)
		if err != nil {
			fmt.Printf("Error inserting terms for document for %s: %s\n", im.item.Url, err)
			continue
		}

		postings := make([]store.Posting, 0, len(uniquePostings))
		for _, posting := range uniquePostings {
			postings = append(postings, posting)
		}

		err = postingStore.InsertMany(postings)
		if err != nil {
			fmt.Printf("Error inserting terms for document for %s: %s\n", im.item.Url, err)
		}
	}
}

// insert each unique term in this document into the term store
// we then have a map of every term to its matching term id in the store.
func (idx *Index) insertTerms(docId int, words []string) (map[string]store.Posting, error) {
	termStore := idx.s.IntoTermStore()
	uniquePostings := make(map[string]store.Posting)
	for _, word := range words {
		// insert term if it doesn't exist
		if p, exists := uniquePostings[word]; !exists {
			termId, err := termStore.Insert(word)
			if err != nil {
				fmt.Printf("Error inserting term %s: %s\n", word, err)
				return nil, err
			}
			newP := store.Posting{
				TermId: termId,
				DocId:  docId,
				TFRaw:  1,
			}
			uniquePostings[word] = newP
		} else {
			p.TFRaw += 1
			uniquePostings[word] = p
		}
	}
	return uniquePostings, nil
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
	idx.processor.Close() // this should cascade through the pipeline.
	idx.wg.Done()
}
