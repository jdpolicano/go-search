package extract

import (
	"errors"
	"io"
	"slices"

	"github.com/pemistahl/lingua-go"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

type HtmlParser struct {
	langs    []lingua.Language
	detector lingua.LanguageDetector
}

var ErrorNotSupportedLanguage = errors.New("Language is not supported")

func NewHtmlParser(langs []lingua.Language) *HtmlParser {
	detector := lingua.
		NewLanguageDetectorBuilder().
		FromAllLanguages().
		WithLowAccuracyMode().
		Build()
	return &HtmlParser{langs, detector}
}

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

// checks the html tag for a "lang" attribute, and validates (if it is there)
// whether or not it is a supported language. The default is to say true,
// so this does not guarrentee that the doc is in a supported language
func (p *HtmlParser) isSupportedLanguageNode(node *html.Node) bool {
	var htmlTagNode *html.Node = nil

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
		return true // we can't say yet that it is NOT supported.
	}

	for _, attr := range htmlTagNode.Attr {
		if attr.Key == "lang" {
			// ISO 639-1 - two language codes
			if len(attr.Val) == 2 {
				isoCode639_1 := lingua.GetIsoCode639_1FromValue(attr.Val)
				attrLang := lingua.GetLanguageFromIsoCode639_1(isoCode639_1)
				return slices.Contains(p.langs, attrLang) // the lang attribute was there, but it isn't a support lang that we know of.
			}

			if len(attr.Val) == 3 {
				isoCode639_3 := lingua.GetIsoCode639_3FromValue(attr.Val)
				attrLang := lingua.GetLanguageFromIsoCode639_3(isoCode639_3)
				return slices.Contains(p.langs, attrLang) // the lang attribute was there, but it isn't a support lang that we know of.
			}

			return false
		}
	}

	return true // again, we don't know for sure, so we should default to true
}

func DfsNodes(n *html.Node, condition func(node *html.Node) bool, cb func(node *html.Node)) {
	if condition(n) {
		cb(n)
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		DfsNodes(c, condition, cb)
	}
}
