// Package crawler contains network utilities for web crawling.
package crawler

import (
	"fmt"
	"io"
	"net/http"
)

// GetReaderFromUrl fetches content from a URL and returns it as an io.Reader.
// It sets appropriate headers and handles HTTP status codes.
func getReaderFromUrl(url string) (io.Reader, error) {
	client := &http.Client{}
	// Create a new request with proper headers
	req, _ := http.NewRequest("GET", url, nil)
	// Set a User-Agent header (required by Wikipedia and many sites)
	// Format: <MyBotName>/<Version> (contact information)
	req.Header.Set("User-Agent", "MyGoScraper/1.0 (jdpolicano@gmail.com)")
	response, ioErr := client.Do(req)
	if ioErr != nil {
		return nil, ioErr
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status error %v", response.StatusCode)
	}

	return response.Body, nil
}
