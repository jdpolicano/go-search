package crawler

import (
	"fmt"
	"sync"

	"github.com/pemistahl/lingua-go"
)

type Index struct {
	queue     *CrawlQueue
	crawler   *Crawler
	processor *Processor
	in        chan string
	wg        *sync.WaitGroup
}

func NewIndex(seeds []string, langs []lingua.Language, wg *sync.WaitGroup) *Index {
	queue := NewCrawlQueue(seeds, wg)
	crawler := NewCrawler(queue.out, wg)
	processor := NewProcessor(crawler.out, queue.in, langs, wg)
	in := processor.index
	return &Index{queue, crawler, processor, in, wg}
}

func (idx *Index) Run() {
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
