package store

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

func ErrorIsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == pgerrcode.UniqueViolation {
			return true
		}
	}

	return false
}

// MakeUrl constructs an absolute URL by resolving a relative URL (href) against a base URL (baseStr).
func MakeUrl(baseStr string, href string) (string, error) {
	// Parse the base URL, which represents the page where the link was found
	base, baseErr := url.Parse(baseStr)
	if baseErr != nil {
		return "", fmt.Errorf("Error parsing baseStr: %v", baseErr)
	}

	// Parse the href, which is the path or URL from the <a> href attribute
	ref, refErr := url.Parse(href)
	if refErr != nil {
		return "", fmt.Errorf("Error parsing href: %v", refErr)
	}

	// Resolve the href relative to the base URL
	resolvedUrl := base.ResolveReference(ref).String()
	return resolvedUrl, nil
}

// Normalizes a URL by:
// - Lowercasing the scheme and host
// - Removing the fragment
// - Sorting query parameters
// - Removing trailing slash (if path is not just "/")
//
// This is the primary key for the 'frontier' table that is used to avoid
// crawling the same URL multiple times.
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

func GetHostame(rawUrl string) (string, error) {
	u, err := url.Parse(rawUrl)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}
