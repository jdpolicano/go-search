package store

import (
	"context"
	"database/sql"
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
const insertDocQuery = `INSERT INTO docs (url, len)
VALUES ($1, $2)
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
	db DBTX
}

func NewDocStore(db DBTX) *DocStore {
	return &DocStore{db}
}

func (ds *DocStore) GetById(ctx context.Context, docId int) (*Doc, error) {
	var doc Doc
	row := ds.db.QueryRow(ctx, "SELECT doc_id, url, title, snippet, len, norm FROM docs WHERE doc_id = $1", docId)
	err := row.Scan(&doc.ID, &doc.Url, &doc.Title, &doc.Snippet, &doc.Len, &doc.Norm)
	return &doc, err
}

func (ds *DocStore) GetByIds(ctx context.Context, docIds []int) ([]*Doc, error) {
	if len(docIds) == 0 {
		return []*Doc{}, nil
	}

	rows, err := ds.db.Query(ctx, "SELECT id, url, title, snippet, len, norm FROM docs WHERE id = ANY($1)", docIds)
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

func (ds *DocStore) InsertDoc(ctx context.Context, url string, len int) (int64, error) {
	var id int64
	err := ds.db.QueryRow(ctx, insertDocQuery, url, len).Scan(&id)
	return id, err
}

func (ds *DocStore) UpdateNorm(ctx context.Context, docId int, norm float64) error {
	_, err := ds.db.Exec(ctx, "UPDATE docs SET norm = $1 WHERE id = $2", norm, docId)
	return err
}
