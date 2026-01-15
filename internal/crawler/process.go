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

type ProcessorMessage struct {
	fi     store.FrontierItem
	reader io.Reader
}

type Processor struct {
	in     chan ProcessorMessage     // accept incoming pages from the crawler
	queue  chan []store.FrontierItem // push more urls to the queue pipeline
	index  chan IndexMessage         // push normalized text input for indexing
	wg     *sync.WaitGroup
	parser *extract.HtmlParser
	s      store.Store
	ctx    context.Context
	cancel context.CancelFunc
}

func NewProcessor(ctx context.Context, cancel context.CancelFunc, s store.Store, in chan ProcessorMessage, queue chan []store.FrontierItem, langs []language.Language, wg *sync.WaitGroup) *Processor {
	index := make(chan IndexMessage)
	parser := extract.NewHtmlParser(langs)
	return &Processor{in, queue, index, wg, parser, s, ctx, cancel}
}

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

func (p *Processor) processMessage(pm ProcessorMessage) {
	doc, parseErr := p.parser.Parse(pm.reader)
	if parseErr != nil {
		p.handleError(pm, parseErr)
		return
	}

	extracted, err := extract.ProcessHtmlDocument(doc)
	if err != nil {
		p.handleError(pm, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	// send to index
	go p.sendToIndex(pm, extracted, &wg)
	// send to queue
	go p.sendToQueue(pm, extracted, &wg)
	// wait for both to be accepted before moving on.
	wg.Wait()
}

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

func (p *Processor) getIndexEntry(pm ProcessorMessage, extracted extract.Extracted) (store.IndexEntry, error) {
	url := pm.fi.Url
	hash := extracted.Hash
	len := extracted.Len
	termFreqs := extracted.TermFreqs
	return store.NewIndexEntry(url, hash, len, termFreqs)
}

func (p *Processor) getFrontierMessages(pc ProcessorMessage, links []string) []store.FrontierItem {
	//time.Sleep(250 * time.Millisecond)
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

func (p *Processor) Close() {
	fmt.Println("Closing Processor")
	close(p.queue)
	close(p.index)
	p.wg.Done()
}
