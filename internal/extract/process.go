package extract

import (
	"fmt"

	"golang.org/x/net/html"
)

type Extracted struct {
	Links           []string
	TermFreqs       map[string]int
	NormalizedWords []string
	Len             int
}

func NewExtracted(links []string, termFreqs map[string]int, wordsNorm []string) Extracted {
	return Extracted{
		Links:           links,
		TermFreqs:       termFreqs,
		NormalizedWords: wordsNorm,
		Len:             len(wordsNorm),
	}
}

func ProcessHtmlDocument(root *html.Node) (Extracted, error) {
	links := make([]string, 0)
	termFreqs := make(map[string]int)
	normWords := make([]string, 0)
	dfsErr := DfsNodes(root, func(node *html.Node) error {
		if isATag(node) {
			for _, attr := range node.Attr {
				if attr.Key == "href" {
					links = append(links, attr.Val)
				}
			}
		}

		if isVisibleText(node) {
			words, scanErr := ScanWordsFromString(node.Data)
			if scanErr != nil {
				fmt.Printf("Error scanning words: %s\n", scanErr)
				return scanErr
			}

			for _, word := range words {
				normWords = append(normWords, word)
				termFreqs[word] += 1
			}
		}

		return nil
	})

	if dfsErr != nil {
		fmt.Printf("Error during DFS of HTML nodes: %s\n", dfsErr)
		return Extracted{}, dfsErr
	}

	return NewExtracted(links, termFreqs, normWords), nil
}
