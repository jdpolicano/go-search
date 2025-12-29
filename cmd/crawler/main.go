package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/jdpolicano/go-search/internal/crawler"
	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/store"
)

func main() {
	s, err := store.NewStore("db/store.db")
	if err != nil {
		fmt.Printf("Error creating store: %s\n", err)
		return
	}
	seeds := []string{"https://en.wikipedia.org/wiki/Computer_science"}
	supportedLangs := []language.Language{language.English}
	wg := sync.WaitGroup{}
	index, err := crawler.NewIndex(s, seeds, supportedLangs, &wg)
	if err != nil {
		fmt.Println(err)
		return
	}
	go index.Run()
	time.Sleep(60 * time.Second)
	index.Close()
	wg.Wait()
}
