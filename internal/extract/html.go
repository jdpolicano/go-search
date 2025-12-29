package extract

import (
	"errors"
	"io"
	"slices"

	"github.com/jdpolicano/go-search/internal/extract/language"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var ErrorNotSupportedLanguage = errors.New("Language is not supported")

type HtmlParser struct {
	langs []language.Language
}

func NewHtmlParser(langs []language.Language) *HtmlParser {
	return &HtmlParser{langs}
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
		// we can't say yet that it is NOT supported.
		//
		// in the future we might use natural language processing
		// to determine the language of the text nodes or something.
		return true
	}

	for _, attr := range htmlTagNode.Attr {
		if attr.Key == "lang" {
			// ISO 639-1 - two language codes
			if len(attr.Val) == 2 {
				isoCode639_1 := language.GetIsoCode639_1FromValue(attr.Val)
				attrLang := language.GetLanguageFromIsoCode639_1(isoCode639_1)
				return slices.Contains(p.langs, attrLang) // the lang attribute was there, but it isn't a support lang that we know of.
			}

			// ISO 639-3 - three language codes
			if len(attr.Val) == 3 {
				isoCode639_3 := language.GetIsoCode639_3FromValue(attr.Val)
				attrLang := language.GetLanguageFromIsoCode639_3(isoCode639_3)
				return slices.Contains(p.langs, attrLang) // the lang attribute was there, but it isn't a support lang that we know of.
			}

			// there is a lang attribute, but we don't know what it is.
			// again, in the future we might use natural language processing, but for now we will just deny this
			// document since it clearly specified a lang attribute that we don't understand.
			return false
		}
	}

	return true // again, we don't know for sure, so we should default to true
}

func GetLinks(n *html.Node) []string {
	links := make([]string, 0, 128)
	seen := make(map[string]bool)
	DfsNodes(n, func(node *html.Node) bool {
		return node.Type == html.ElementNode && node.DataAtom == atom.A
	}, func(a *html.Node) error {
		for _, attr := range a.Attr {
			if attr.Key == "href" {
				if _, alreadySeen := seen[attr.Val]; !alreadySeen {
					links = append(links, attr.Val)
					seen[attr.Val] = true
				}
			}
		}
		return nil
	})
	return links
}

func NewTextNodeReader(n *html.Node) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		DfsNodes(n, func(node *html.Node) bool {
			return node.Type == html.TextNode
		}, func(textNode *html.Node) error {
			// todo: should we handle errors here?
			_, e := pw.Write([]byte(textNode.Data + " "))
			return e
		})
	}()

	return pr
}

func DfsNodes(n *html.Node, condition func(node *html.Node) bool, cb func(node *html.Node) error) error {
	if condition(n) {
		if err := cb(n); err != nil {
			return err
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if err := DfsNodes(c, condition, cb); err != nil {
			return err
		}
	}
	return nil
}
