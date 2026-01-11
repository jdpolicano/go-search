package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jdpolicano/go-search/internal/crawler"
	"github.com/jdpolicano/go-search/internal/extract/language"
	"github.com/jdpolicano/go-search/internal/store"
	"github.com/joho/godotenv"
)

func main() {
	// Load the .env file
	err := godotenv.Load()
	if err != nil {
		// Log a fatal error if the file cannot be loaded
		log.Fatalf("Error loading .env file: %s", err)
	}

	s, err := store.NewStore("db/store.db")
	if err != nil {
		fmt.Printf("Error creating store: %s\n", err)
		return
	}
	seeds := []string{
		"https://en.wikipedia.org/wiki/Artificial_intelligence",
		"https://en.wikipedia.org/wiki/C_(programming_language)",
		"https://en.wikipedia.org/wiki/Google_Search",
		"https://en.wikipedia.org/wiki/Computer",
		"https://en.wikipedia.org/wiki/Python_(programming_language)",
		"https://en.wikipedia.org/wiki/Sergey_Brin",
		"https://en.wikipedia.org/wiki/Cloud_computing",
		"https://en.wikipedia.org/wiki/Generative_pre-trained_transformer",
		"https://en.wikipedia.org/wiki/HTML",
		"https://en.wikipedia.org/wiki/Generative_artificial_intelligence",
		"https://en.wikipedia.org/wiki/Video_game",
		"https://en.wikipedia.org/wiki/Quantum_computing",
		"https://en.wikipedia.org/wiki/R_(programming_language)",
		"https://en.wikipedia.org/wiki/Machine_learning",
		"https://en.wikipedia.org/wiki/JavaScript",
		"https://en.wikipedia.org/wiki/C%2B%2B",
		"https://en.wikipedia.org/wiki/Go_(programming_language)",
		"https://en.wikipedia.org/wiki/Rust_(programming_language)",
		"https://en.wikipedia.org/wiki/Java_(programming_language)",
		"https://en.wikipedia.org/wiki/Blockchain",
		"https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm",
		"https://en.wikipedia.org/wiki/JSON",
		"https://en.wikipedia.org/wiki/CSS",
		"https://en.wikipedia.org/wiki/Unix",
		"https://en.wikipedia.org/wiki/Computer_science",
		"https://en.wikipedia.org/wiki/Programmer",
		"https://en.wikipedia.org/wiki/Software",
	}
	supportedLangs := []language.Language{language.English}
	wg := sync.WaitGroup{}
	index, err := crawler.NewIndex(s, seeds, supportedLangs, &wg)
	if err != nil {
		fmt.Println(err)
		return
	}
	go index.Run()
	time.Sleep(60 * time.Second * 3)
	index.Close()
	wg.Wait()
}
