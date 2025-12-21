package crawler

import (
	"fmt"
	"io"
	"net/http"
)

// Defines a remote resource.
type Resource interface {
	GetReader() (io.Reader, error)
	// this is the formal path of the Resource, generic over different resource types
	Name() string
}

type UrlResource struct {
	url string
}

func NewUrlResource(url string) *UrlResource {
	return &UrlResource{url}
}

func (u *UrlResource) GetReader() (io.Reader, error) {
	client := &http.Client{}
	// 1. Create a new request
	req, _ := http.NewRequest("GET", u.url, nil)
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

func (u *UrlResource) Name() string {
	return u.url
}
