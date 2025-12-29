package store

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/mattn/go-sqlite3"
)

func ErrorIsConstraintViolation(err error) bool {
	if err == nil {
		return false
	}

	var sqlite3Err sqlite3.Error
	if errors.As(err, &sqlite3Err) {
		return sqlite3Err.Code == sqlite3.ErrConstraint
	}

	return false
}

func ErrorIsForeignKeyViolation(err error) bool {
	if err == nil {
		return false
	}

	var sqlite3Err sqlite3.Error
	if errors.As(err, &sqlite3Err) {
		return sqlite3Err.ExtendedCode == sqlite3.ErrConstraintForeignKey
	}

	return false
}

// handles resolving relative and absolute urls etc...
func MakeUrl(baseStr string, href string) (string, error) {
	// The URL of the page where the link was found
	base, baseErr := url.Parse(baseStr)
	if baseErr != nil {
		return "", fmt.Errorf("Error parsing baseStr: %d", baseErr)
	}
	// The path from the <a> href attribute
	ref, refErr := url.Parse(href)
	if refErr != nil {
		return "", fmt.Errorf("Error parsing refUrl: %d", refErr)
	}
	// Resolve the reference
	resolvedUrl := base.ResolveReference(ref).String()
	return resolvedUrl, nil
}

func NormalizeURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	// Lowercase scheme and host
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)

	// Remove fragment
	u.Fragment = ""

	// Sort query parameters
	query := u.Query()
	for key, values := range query {
		sort.Strings(values)
		query[key] = values
	}
	u.RawQuery = query.Encode()

	// Remove trailing slash if path is not just "/"
	if u.Path != "/" && strings.HasSuffix(u.Path, "/") {
		u.Path = strings.TrimSuffix(u.Path, "/")
	}

	return u.String(), nil
}
