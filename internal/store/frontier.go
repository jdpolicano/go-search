package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const insertFIBatchStmt = `INSERT INTO frontier (url, url_norm, parent_url, depth, status)
SELECT fi.url, fi.url_norm, fi.parent_url, fi.depth, fi.status
FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::int[])
	 AS fi(url, url_norm, parent_url, depth, status)
ON CONFLICT (url_norm) DO NOTHING;`

type FrontierStatusEnum int

const (
	StatusUnvisited FrontierStatusEnum = iota
	StatusInProgress
	StatusCompleted
	StatusFailed
)

type FrontierItem struct {
	Url       string
	UrlNorm   string
	ParentUrl string
	Depth     int
	Status    FrontierStatusEnum
}

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

func NewFrontierItemFromSeed(url string) (FrontierItem, error) {
	urlNorm, err := NormalizeURL(url)
	return FrontierItem{url, urlNorm, "", 0, StatusUnvisited}, err
}

func NewFrontierItem(url, urlNorm, parentUrl string, depth int, status FrontierStatusEnum) FrontierItem {
	return FrontierItem{url, urlNorm, parentUrl, depth, status}
}

func (fi *FrontierItem) FromRows(row pgx.Rows) error {
	return row.Scan(&fi.Url, &fi.UrlNorm, &fi.ParentUrl, &fi.Depth, &fi.Status)
}

func (fi *FrontierItem) String() string {
	return fi.Url
}

func GetFICount(ctx context.Context, db DBTX) (int, error) {
	var count int
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM frontier")
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func GetFICountByStatus(ctx context.Context, db DBTX, status FrontierStatusEnum) (int, error) {
	var count int
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM frontier WHERE status = $1", status)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func GetFIByStatusDepthSorted(ctx context.Context, db DBTX, status FrontierStatusEnum, limit int) ([]FrontierItem, error) {
	rows, err := db.Query(ctx, "SELECT * FROM frontier WHERE status = $1 ORDER BY depth ASC LIMIT $2", status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fi := make([]FrontierItem, limit)
	cnt := 0
	for ; rows.Next(); cnt++ {
		if err := (&fi[cnt]).FromRows(rows); err != nil {
			return nil, err
		}
	}
	return fi[:cnt], nil
}

func InsertFI(ctx context.Context, db DBTX, item FrontierItem) error {
	_, err := db.Exec(ctx, "INSERT INTO frontier (url, url_norm, parent_url, depth, status) VALUES ($1, $2, $3, $4, $5)", item.Url, item.UrlNorm, item.ParentUrl, item.Depth, item.Status)
	return err
}

func InsertFIBatch(ctx context.Context, db DBTX, items []FrontierItem) error {
	if len(items) == 0 {
		return nil
	}

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

	_, err := db.Exec(ctx, insertFIBatchStmt, urls, urlNorms, parentUrls, depths, statuses)
	return err
}

func UpdateFIStatus(ctx context.Context, db DBTX, urlNorm string, status FrontierStatusEnum) error {
	_, err := db.Exec(ctx, "UPDATE frontier SET status = $1 WHERE url_norm = $2", status, urlNorm)
	return err
}

func CleanupFrontier(ctx context.Context, db DBTX) error {
	_, err := db.Exec(ctx, "DELETE FROM frontier WHERE status = $1", StatusCompleted)
	return err
}
