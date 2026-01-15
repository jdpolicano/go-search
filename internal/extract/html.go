// Package extract provides HTML parsing and content extraction functionality for the search engine.
package extract

import (
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/jdpolicano/go-search/internal/extract/language"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// ErrorNotSupportedLanguage is returned when a document's language is not supported.
var ErrorNotSupportedLanguage = errors.New("Language is not supported")

// HtmlParser parses HTML documents and validates language support.
type HtmlParser struct {
	langs []language.Language // Supported languages for content extraction
}

// NewHtmlParser creates a new HtmlParser instance with the given supported languages.
func NewHtmlParser(langs []language.Language) *HtmlParser {
	return &HtmlParser{langs}
}

// Parse parses an HTML document from the given reader and validates language support.
func (p *HtmlParser) Parse(reader io.Reader) (*html.Node, error) {
	doc, parseErr := html.Parse(reader)
	if parseErr != nil {
		return nil, parseErr
	}

	if !p.isSupportedLanguageNode(doc) {
		return nil, ErrorNotSupportedLanguage
	}

	return doc, nil
}

// isSupportedLanguageNode checks the html tag for a "lang" attribute and validates language support.
// The default is to return true, so this does not guarantee that the doc is in a supported language.
func (p *HtmlParser) isSupportedLanguageNode(node *html.Node) bool {
	var htmlTagNode *html.Node = nil

	// Find the HTML tag node
	if node.Type == html.DocumentNode {
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			if c.DataAtom == atom.Html {
				htmlTagNode = c
				break
			}
		}
	} else if node.DataAtom == atom.Html {
		htmlTagNode = node
	}

	if htmlTagNode == nil {
		// We can't determine language support yet.
		// Future enhancement: use natural language processing to detect language from text content.
		return true
	}

	// Check for lang attribute and validate against supported languages
	for _, attr := range htmlTagNode.Attr {
		if attr.Key == "lang" {
			// ISO 639-1 - two letter language codes
			if len(attr.Val) == 2 {
				isoCode639_1 := language.GetIsoCode639_1FromValue(attr.Val)
				attrLang := language.GetLanguageFromIsoCode639_1(isoCode639_1)
				return slices.Contains(p.langs, attrLang)
			}

			// ISO 639-3 - three letter language codes
			if len(attr.Val) == 3 {
				isoCode639_3 := language.GetIsoCode639_3FromValue(attr.Val)
				attrLang := language.GetLanguageFromIsoCode639_3(isoCode639_3)
				return slices.Contains(p.langs, attrLang)
			}

			// Lang attribute exists but we don't recognize it.
			// Future enhancement: use NLP to detect language, but for now deny the document.
			return false
		}
	}

	return true // Default to true when no lang attribute is found
}

// isATag checks if a node is an HTML anchor (<a>) tag.
func isATag(node *html.Node) bool {
	return node.Type == html.ElementNode && node.DataAtom == atom.A
}

// isVisibleText determines if a text node contains visible content.
// It filters out script/style content and whitespace-only nodes.
func isVisibleText(n *html.Node) bool {
	// 1. Must be a text node
	if n.Type != html.TextNode {
		return false
	}

	// 2. Check parent to see if it's a "hidden" tag
	if n.Parent != nil && n.Parent.Type == html.ElementNode {
		tag := strings.ToLower(n.Parent.Data)
		// Blacklist tags that contain non-visible text
		if tag == "script" || tag == "style" || tag == "head" || tag == "noscript" {
			return false
		}
	}

	// 3. Filter out nodes that are just whitespace (newlines/tabs)
	if strings.TrimSpace(n.Data) == "" {
		return false
	}

	return true
}

// DfsNodes performs a depth-first traversal of HTML nodes, calling the callback for each node.
func DfsNodes(n *html.Node, cb func(node *html.Node) error) error {
	if n == nil {
		return nil
	}

	if err := cb(n); err != nil {
		return err
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if err := DfsNodes(c, cb); err != nil {
			return err
		}
	}

	return nil
}
