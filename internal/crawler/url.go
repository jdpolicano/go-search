package crawler

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
)

type QueueItem struct {
	url  string
	href string
}

func (qi QueueItem) String() string {
	return fmt.Sprintf("{Url: %s Href %s}", qi.url, qi.href)
}

type CrawlQueue struct {
	queue   []string
	in      chan []QueueItem // data into the queue, for a bfs queue.
	out     chan string      // send an item along the queue
	visited map[string]bool  // we don't want to produce url's we've already seen
	wg      *sync.WaitGroup
}

func NewCrawlQueue(seeds []string, wg *sync.WaitGroup) *CrawlQueue {
	in, out := make(chan []QueueItem), make(chan string)
	visited := make(map[string]bool)
	return &CrawlQueue{seeds, in, out, visited, wg}
}

func (cq *CrawlQueue) Run() {
	defer cq.Close()
	if len(cq.queue) == 0 {
		return
	}

	for {
		var activeOut chan string
		var top string

		if len(cq.queue) > 0 {
			activeOut = cq.out
			top = cq.queue[0]
		} else {
			// Queue is empty: activeOut stays nil, so the 'case activeOut <-'
			// will be ignored until new items arrive via cq.in.
			activeOut = nil
		}
		select {
		// a url is accepted by the downstream
		case activeOut <- top:
			{
				fmt.Printf("Starting %s\n", top)
				cq.queue = cq.queue[1:]
			}
		case items, ok := <-cq.in:
			{
				if !ok {
					fmt.Printf("Input channel closed, items left %v\n", len(cq.queue))
					return
				}

				urls := make([]string, 0, len(items))
				for _, item := range items {
					resolvedUrl, pErr := MakeUrl(item.url, item.href)
					if pErr != nil {
						fmt.Printf("Error parsing url %d\n", pErr)
						continue
					}

					norm, nErr := normalizeURL(resolvedUrl)
					if nErr != nil {
						fmt.Printf("Error normalizing url %d\n", nErr)
					}
					if _, seen := cq.visited[norm]; !seen {
						// fmt.Println("Resolved: ", resolvedUrl)
						// fmt.Println("Norm:     ", norm)
						cq.visited[norm] = true
						urls = append(urls, resolvedUrl)
					}
				}
				cq.queue = append(cq.queue, urls...)
			}
		}
	}
}

func (cq *CrawlQueue) Close() {
	fmt.Println("Closing UrlQueue")
	close(cq.out)
	cq.wg.Done()
}

// handles resolving relative and absolute urls etc...
func MakeUrl(baseStr string, href string) (string, error) {
	// The URL of the page where the link was found
	base, baseErr := url.Parse(baseStr)
	if baseErr != nil {
		return "", fmt.Errorf("Error parsing baseStr: %d", baseErr)
	}
	// The path from the <a> href attribute
	ref, refErr := url.Parse(href)
	if refErr != nil {
		return "", fmt.Errorf("Error parsing refUrl: %d", refErr)
	}
	// Resolve the reference
	resolvedUrl := base.ResolveReference(ref).String()
	return resolvedUrl, nil
}

func normalizeURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	// Lowercase scheme and host
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)

	// Remove fragment
	u.Fragment = ""

	// Sort query parameters
	query := u.Query()
	for key, values := range query {
		sort.Strings(values)
		query[key] = values
	}
	u.RawQuery = query.Encode()

	// Remove trailing slash if path is not just "/"
	if u.Path != "/" && strings.HasSuffix(u.Path, "/") {
		u.Path = strings.TrimSuffix(u.Path, "/")
	}

	return u.String(), nil
}
