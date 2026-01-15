// Package extract provides content processing and extraction functionality.
package extract

import (
	"crypto"
	"encoding/hex"

	"golang.org/x/net/html"
)

// Extracted contains the processed content from an HTML document.
type Extracted struct {
	Links     []string       // Extracted links (href attributes)
	TermFreqs map[string]int // Term frequency map for the document
	Hash      string         // SHA256 hash of all words for content deduplication
	Len       int            // Total number of words in the document
}

// ProcessHtmlDocument extracts links, text, and metadata from an HTML document.
// It performs a depth-first traversal to collect href attributes and visible text.
func ProcessHtmlDocument(root *html.Node) (Extracted, error) {
	links := make([]string, 0)
	termFreqs := make(map[string]int)
	hash := crypto.SHA256.New()
	len := 0

	// Traverse the HTML document and extract content
	dfsErr := DfsNodes(root, func(node *html.Node) error {
		// Extract links from anchor tags
		if isATag(node) {
			for _, attr := range node.Attr {
				if attr.Key == "href" {
					links = append(links, attr.Val)
				}
			}
		}

		// Process visible text content
		if isVisibleText(node) {
			words, scanErr := ScanWordsFromString(node.Data)
			if scanErr != nil {
				return scanErr
			}

			// Update term frequencies and hash
			for _, word := range words {
				hash.Write([]byte(word))
				termFreqs[word] += 1
				len += 1
			}
		}

		return nil
	})

	if dfsErr != nil {
		return Extracted{}, dfsErr
	}

	return Extracted{
		Links:     links,
		TermFreqs: termFreqs,
		Hash:      hex.EncodeToString(hash.Sum(nil)),
		Len:       len,
	}, nil
}
