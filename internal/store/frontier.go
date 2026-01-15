// Package store provides database operations and data structures for the search engine.
package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const insertFIBatchStmt = `INSERT INTO frontier (url, url_norm, parent_url, depth, status)
SELECT fi.url, fi.url_norm, fi.parent_url, fi.depth, fi.status
FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::int[])
	 AS fi(url, url_norm, parent_url, depth, status)
ON CONFLICT (url_norm) DO NOTHING
RETURNING url, url_norm, parent_url, depth, status;`

// FrontierStatusEnum represents the status of a frontier item in the crawling process.
type FrontierStatusEnum int

const (
	StatusUnvisited  FrontierStatusEnum = iota // URL has not been crawled yet
	StatusInProgress                           // URL is currently being crawled
	StatusCompleted                            // URL has been successfully crawled
	StatusFailed                               // URL crawling failed
)

// FrontierItem represents a URL to be crawled with metadata for the crawling process.
type FrontierItem struct {
	Url       string             // Original URL
	UrlNorm   string             // Normalized URL for deduplication
	ParentUrl string             // URL of the page that contained this link
	Depth     int                // Depth in the crawling tree
	Status    FrontierStatusEnum // Current status of this URL
}

// NewFrontierItemFromParent creates a new frontier item from a parent URL and relative link.
func NewFrontierItemFromParent(parent FrontierItem, rawUrl string) (FrontierItem, error) {
	url, err := MakeUrl(parent.Url, rawUrl)
	if err != nil {
		return FrontierItem{}, err
	}
	urlNorm, err := NormalizeURL(url)
	if err != nil {
		return FrontierItem{}, err
	}
	return FrontierItem{url, urlNorm, parent.Url, parent.Depth + 1, StatusUnvisited}, err
}

// NewFrontierItemFromSeed creates a new frontier item from a seed URL with depth 0.
func NewFrontierItemFromSeed(url string) (FrontierItem, error) {
	urlNorm, err := NormalizeURL(url)
	return FrontierItem{url, urlNorm, "", 0, StatusUnvisited}, err
}

// NewFrontierItem creates a new frontier item with all specified fields.
func NewFrontierItem(url, urlNorm, parentUrl string, depth int, status FrontierStatusEnum) FrontierItem {
	return FrontierItem{url, urlNorm, parentUrl, depth, status}
}

// FromRows populates a FrontierItem from database query results.
func (fi *FrontierItem) FromRows(rows pgx.Rows) error {
	return rows.Scan(&fi.Url, &fi.UrlNorm, &fi.ParentUrl, &fi.Depth, &fi.Status)
}

// GetFICount returns the total count of frontier items.
func GetFICount(ctx context.Context, db DBTX) (int, error) {
	var count int
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM frontier")
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// GetFICountByStatus returns the count of frontier items with a specific status.
func GetFICountByStatus(ctx context.Context, db DBTX, status FrontierStatusEnum) (int, error) {
	var count int
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM frontier WHERE status = $1", status)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// GetFIByStatusDepthSorted returns frontier items sorted by depth for breadth-first crawling.
func GetFIByStatusDepthSorted(ctx context.Context, db DBTX, status FrontierStatusEnum, limit int) (pgx.Rows, error) {
	rows, err := db.Query(ctx, "SELECT * FROM frontier WHERE status = $1 ORDER BY depth ASC LIMIT $2", status, limit)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// InsertFI inserts a single frontier item into the database.
func InsertFI(ctx context.Context, db DBTX, item FrontierItem) error {
	_, err := db.Exec(ctx, "INSERT INTO frontier (url, url_norm, parent_url, depth, status) VALUES ($1, $2, $3, $4, $5)", item.Url, item.UrlNorm, item.ParentUrl, item.Depth, item.Status)
	return err
}

// InsertFIBatch inserts multiple frontier items in a single database operation for efficiency.
func InsertFIBatch(ctx context.Context, db DBTX, items []FrontierItem) ([]FrontierItem, error) {
	urls := make([]string, len(items))
	urlNorms := make([]string, len(items))
	parentUrls := make([]string, len(items))
	depths := make([]int, len(items))
	statuses := make([]int, len(items))

	for i, fi := range items {
		urls[i] = fi.Url
		urlNorms[i] = fi.UrlNorm
		parentUrls[i] = fi.ParentUrl
		depths[i] = fi.Depth
		statuses[i] = int(fi.Status)
	}

	rows, err := db.Query(ctx, insertFIBatchStmt, urls, urlNorms, parentUrls, depths, statuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var insertedItems []FrontierItem
	for rows.Next() {
		var fi FrontierItem
		if err := fi.FromRows(rows); err != nil {
			return nil, err
		}
		insertedItems = append(insertedItems, fi)
	}
	return insertedItems, nil
}

// updates the status of a frontier item identified by its normalized URL.
func UpdateFIStatus(ctx context.Context, db DBTX, urlNorm string, status FrontierStatusEnum) error {
	_, err := db.Exec(ctx, "UPDATE frontier SET status = $1 WHERE url_norm = $2", status, urlNorm)
	return err
}

// CleanupFrontier removes completed frontier items from the database to free space.
func CleanupFrontier(ctx context.Context, db DBTX) error {
	_, err := db.Exec(ctx, "DELETE FROM frontier WHERE status = $1", StatusCompleted)
	return err
}
