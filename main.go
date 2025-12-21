package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jdpolicano/go-vec-search/internal/document"
)

type SearchItem struct {
	doc        *document.Document
	similarity float64
}

func getStopWords(p string) (map[string]any, error) {
	f, ioErr := os.Open(p)
	if ioErr != nil {
		return nil, ioErr
	}
	defer f.Close()
	scanner := document.NewTextDocumentScanner(f)
	stopWords := make(map[string]any, 1024)
	if err := document.ForEachWord(scanner, func(w string) {
		stopWords[w] = nil
	}); err != nil {
		return nil, err
	}
	return stopWords, nil
}

func getAllDocumentPaths(root, ext string) ([]string, []error) {
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	fileSys := os.DirFS(".")
	filePaths := make([]string, 0, 128)
	errors := make([]error, 0, 128)

	fs.WalkDir(fileSys, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errors = append(errors, err)
			return err
		}

		if d.IsDir() {
			return nil
		}

		if filepath.Ext(path) == ext {
			filePaths = append(filePaths, path)
		}

		return nil
	})

	if len(errors) > 0 {
		for _, err := range errors {
			fmt.Println(err)
		}
		return nil, errors
	}

	return filePaths, nil
}

func main() {
	root := "assets"
	filePaths, errors := getAllDocumentPaths(root, ".md")

	if len(errors) > 0 {
		return
	}

	if len(filePaths) == 0 {
		fmt.Printf("no filepaths matching root \"%s\"\n", root)
	}

	stopWords, err := getStopWords("stop_words.txt")
	if err != nil {
		fmt.Println(err)
		return
	}

	documents := make([]*document.Document, 0, 128)
	for _, path := range filePaths {
		f, ioErr := os.Open(path)
		if ioErr != nil {
			fmt.Println(ioErr)
			return
		}
		name := filepath.Base(path)
		doc, err := document.NewDocument(name, stopWords, f)
		if err != nil {
			fmt.Println(err)
			return
		}
		documents = append(documents, doc)
	}

	idfMap := document.GetIdfMap(documents)
	for word, idf := range idfMap {
		fmt.Printf("(%s) -> %f\n", word, idf)
	}
	search := make([]SearchItem, 0, len(documents))
	queryReader := strings.NewReader("how to export functions")
	query, qErr := document.NewDocument("query", stopWords, queryReader)
	if qErr != nil {
		fmt.Println(qErr)
		return
	}
	queryVec, _ := document.NewVector(query, idfMap, 10)
	for _, doc := range documents {
		docVec, docVecErr := document.NewVector(doc, idfMap, 1024)
		if docVecErr != nil {
			fmt.Println(docVecErr)
			continue
		}
		similarity := queryVec.CosineSimilarity(docVec)
		search = append(search, SearchItem{doc, similarity})
	}
	sort.Slice(search, func(i, j int) bool {
		return search[i].similarity < search[j].similarity
	})

	for _, s := range search {
		fmt.Printf("(%s) similarity: %f\n", s.doc.Path, s.similarity)
	}
}
