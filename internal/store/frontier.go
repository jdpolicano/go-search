package store

import "database/sql"

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

func (fi FrontierItem) FromRows(row *sql.Rows) error {
	return row.Scan(&fi.Url, &fi.UrlNorm, &fi.ParentUrl, &fi.Depth, &fi.Status)
}

func (fi FrontierItem) String() string {
	return fi.Url
}

type FrontierStore struct {
	db *sql.DB
}

func NewFrontierStore(db *sql.DB) *FrontierStore {
	return &FrontierStore{db}
}

func (fs *FrontierStore) GetCount() (int, error) {
	var count int
	row := fs.db.QueryRow("SELECT COUNT(*) FROM frontier")
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (fs *FrontierStore) GetCountByStatus(status FrontierStatusEnum) (int, error) {
	var count int
	row := fs.db.QueryRow("SELECT COUNT(*) FROM frontier WHERE status = ?", status)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (fs *FrontierStore) GetByStatusDepthSorted(status FrontierStatusEnum, limit int) ([]FrontierItem, error) {
	rows, err := fs.db.Query("SELECT * FROM frontier WHERE status = ? ORDER BY depth ASC LIMIT ?", status, limit)
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

func (fs *FrontierStore) Insert(item FrontierItem) error {
	_, err := fs.db.Exec("INSERT INTO frontier (url, url_norm, parent_url, depth, status) VALUES (?, ?, ?, ?, ?)", item.Url, item.UrlNorm, item.ParentUrl, item.Depth, item.Status)
	return err
}

func (fs *FrontierStore) InsertMany(items []FrontierItem) error {
	tx, err := fs.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO frontier (url, url_norm, parent_url, depth, status) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, item := range items {
		if _, err := stmt.Exec(item.Url, item.UrlNorm, item.ParentUrl, item.Depth, item.Status); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (fs *FrontierStore) UpdateStatus(urlNorm string, status FrontierStatusEnum) error {
	_, err := fs.db.Exec("UPDATE frontier SET status = ? WHERE url_norm = ?", status, urlNorm)
	return err
}

func (fs *FrontierStore) Cleanup() error {
	_, err := fs.db.Exec("DELETE FROM frontier WHERE status = ?", StatusCompleted)
	return err
}
