package store

import (
	"database/sql"
	"fmt"
	"strings"
)

type Doc struct {
	ID      int64
	Url     string
	Title   sql.NullString
	Snippet sql.NullString
	Len     int
	Norm    sql.NullFloat64
}

// upsert a doc with a dummy update to get doc_id on conflict
// in the future we might want to update title/snippet if they change
const InsertDocQuery = `INSERT INTO docs (url, len)
VALUES (?, ?)
ON CONFLICT(url) DO UPDATE SET
	len = EXCLUDED.len
RETURNING id;`

// safety: this inits norm to null by default and sets other fields to the
// golang zero value if not provided
func NewDoc(url string, length int) Doc {
	return Doc{
		Url: url,
		Len: length,
	}
}

type DocStore struct {
	db *sql.DB
}

func NewDocStore(db *sql.DB) *DocStore {
	return &DocStore{db}
}

func (ds *DocStore) GetById(docId int) (*Doc, error) {
	var doc Doc
	row := ds.db.QueryRow("SELECT doc_id, url, title, snippet, len, norm FROM docs WHERE doc_id = ?", docId)
	err := row.Scan(&doc.ID, &doc.Url, &doc.Title, &doc.Snippet, &doc.Len, &doc.Norm)
	return &doc, err
}

func (ds *DocStore) GetByIds(docIds []int) ([]*Doc, error) {
	if len(docIds) == 0 {
		return []*Doc{}, nil
	}
	placeholders := make([]string, len(docIds))
	args := make([]any, len(docIds))
	for i, _ := range docIds {
		placeholders[i] = "?"
		args[i] = docIds[i]
	}
	placeHolderStr := strings.Join(placeholders, ", ")
	query := fmt.Sprintf("SELECT id, url, title, snippet, len, norm FROM docs WHERE id IN (%s)", placeHolderStr)
	rows, err := ds.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	docs := make([]*Doc, 0, len(docIds))
	for rows.Next() {
		var doc Doc
		if err := rows.Scan(&doc.ID, &doc.Url, &doc.Title, &doc.Snippet, &doc.Len, &doc.Norm); err != nil {
			return nil, err
		}
		docs = append(docs, &doc)
	}
	return docs, nil
}

func (ds *DocStore) Insert(url string, len int) (int64, error) {
	var id int64
	err := ds.db.QueryRow(InsertDocQuery, url, len).Scan(&id)
	return id, err
}

func (ds *DocStore) UpdateNorm(docId int, norm float64) error {
	_, err := ds.db.Exec("UPDATE docs SET norm = ? WHERE id = ?", norm, docId)
	return err
}
