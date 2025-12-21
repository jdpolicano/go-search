package crawler

import (
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jdpolicano/go-vec-search/internal/extract"
	"github.com/pemistahl/lingua-go"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

type PageContent struct {
	url    string
	reader io.Reader
}

type Processor struct {
	in     chan PageContent // accept incoming pages from the crawler
	queue  chan []QueueItem // push more urls to the queue pipeline
	index  chan string      // push normalized text input for indexing
	wg     *sync.WaitGroup
	parser *extract.HtmlParser
}

func NewProcessor(in chan PageContent, queue chan []QueueItem, langs []lingua.Language, wg *sync.WaitGroup) *Processor {
	index := make(chan string)
	parser := extract.NewHtmlParser(langs)
	return &Processor{in, queue, index, wg, parser}
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
			fmt.Printf("Error processing html from %s: %s\n", pc.url, parseErr)
			continue
		}

		p.recurse(pc, doc)
	}
}

func (p *Processor) recurse(pc PageContent, n *html.Node) {
	links := make([]QueueItem, 0, 128)
	extract.DfsNodes(n, isATag, func(a *html.Node) {
		for _, attr := range a.Attr {
			if attr.Key == "href" {
				if !strings.HasPrefix(attr.Val, "#") {
					qi := QueueItem{pc.url, attr.Val}
					links = append(links, qi)
				}
			}
		}
	})
	time.Sleep(1 * time.Second)
	p.queue <- links
}

func (p *Processor) Close() {
	fmt.Println("Closing Processor")
	close(p.queue)
	close(p.index)
	p.wg.Done()
}

func printATags(n *html.Node) {
	extract.DfsNodes(n, isATag, func(a *html.Node) {
		for _, attr := range a.Attr {
			if attr.Key == "href" {
				u, e := url.Parse(attr.Val)
				if e != nil {
					fmt.Println(e)
				}

				fmt.Printf("PATH: %s\n", u.Path)
			}
		}
	})
}

func isATag(n *html.Node) bool {
	return n.Type == html.ElementNode && n.DataAtom == atom.A
}
