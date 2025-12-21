package main

import (
	"sync"
	"time"

	"github.com/jdpolicano/go-vec-search/internal/crawler"
	"github.com/pemistahl/lingua-go"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

func isATag(n *html.Node) bool {
	return n.Type == html.ElementNode && n.DataAtom == atom.A
}

func main() {
	seeds := []string{"https://en.wikipedia.org/wiki/Computer_science"}
	supportedLangs := []lingua.Language{lingua.English}
	wg := sync.WaitGroup{}
	index := crawler.NewIndex(seeds, supportedLangs, &wg)
	go index.Run()
	time.Sleep(30 * time.Second)
	index.Close()
	wg.Wait()
}
