package crawler

import (
	"fmt"
	"io"
	"sync"

	"github.com/jdpolicano/go-search/internal/extract"
	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/store"
	"golang.org/x/net/html"
)

type ProcessorMessage struct {
	item   store.FrontierItem
	reader io.Reader
}

type Processor struct {
	s      *store.Store
	in     chan ProcessorMessage     // accept incoming pages from the crawler
	queue  chan []store.FrontierItem // push more urls to the queue pipeline
	index  chan IndexMessage         // push normalized text input for indexing
	wg     *sync.WaitGroup
	parser *extract.HtmlParser
}

func NewProcessor(s *store.Store, in chan ProcessorMessage, queue chan []store.FrontierItem, langs []language.Language, wg *sync.WaitGroup) *Processor {
	index := make(chan IndexMessage)
	parser := extract.NewHtmlParser(langs)
	return &Processor{s, in, queue, index, wg, parser}
}

func (p *Processor) Run() {
	defer p.Close()
	for {
		pc, ok := <-p.in
		if !ok {
			fmt.Println("Processor \"in\" channel closed")
			return
		}

		doc, parseErr := p.parser.Parse(pc.reader)
		if parseErr != nil {
			p.handleParseError(pc, parseErr)
			continue
		}
		// todo send to render queue
		p.extractLinks(pc, doc)
		p.sendToIndex(pc, doc)
	}
}

func (p *Processor) handleParseError(pc ProcessorMessage, err error) {
	fmt.Printf("%s: %s\n", pc.item.Url, err)
	e := p.s.IntoFrontierStore().UpdateStatus(pc.item.UrlNorm, store.StatusFailed)
	if e != nil {
		fmt.Printf("Error updating status to failed for %s: %s\n", pc.item.UrlNorm, e)
	}
}

func (p *Processor) extractLinks(pc ProcessorMessage, n *html.Node) {
	links := extract.GetLinks(n)
	//time.Sleep(250 * time.Millisecond)
	items := make([]store.FrontierItem, 0, len(links))
	for _, link := range links {
		item, err := store.NewFrontierItemFromParent(pc.item, link)
		if err != nil {
			fmt.Println(err)
			continue
		}
		items = append(items, item)
	}
	p.queue <- items
}

func (p *Processor) sendToIndex(pc ProcessorMessage, n *html.Node) error {
	textNodeReader := extract.NewTextNodeReader(n)
	words, err := extract.ScanWords(textNodeReader)
	if err != nil {
		return err
	}
	p.index <- IndexMessage{pc.item, words}
	return nil
}

func (p *Processor) Close() {
	fmt.Println("Closing Processor")
	close(p.queue)
	close(p.index)
	p.wg.Done()
}
