package crawler

import (
	"fmt"
	"sync"
)

type Crawler struct {
	in  chan string
	out chan PageContent
	wg  *sync.WaitGroup
}

func NewCrawler(in chan string, wg *sync.WaitGroup) *Crawler {
	out := make(chan PageContent)
	return &Crawler{in, out, wg}
}

func (c *Crawler) Run() {
	defer c.Close()
	for {
		url, ok := <-c.in
		if !ok {
			fmt.Println("Crawler \"in\" channel closed, returning")
			return
		}
		fmt.Println("Crawler handling url: ", url)
		ur := NewUrlResource(url)
		ioReader, ioErr := ur.GetReader()
		if ioErr != nil {
			fmt.Printf("Error getting resource: %s", ur.Name())
			continue
		}
		c.out <- PageContent{ur.Name(), ioReader}
	}
}

func (c *Crawler) Close() {
	fmt.Println("Closing crawler")
	close(c.out)
	c.wg.Done()
}
