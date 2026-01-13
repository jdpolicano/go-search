package crawler

import (
	"fmt"
	"io"
	"net/http"
)

func GetReaderFromUrl(url string) (io.Reader, error) {
	client := &http.Client{}
	// 1. Create a new request
	req, _ := http.NewRequest("GET", url, nil)
	// 2. Set a User-Agent header (required by Wikipedia)
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
