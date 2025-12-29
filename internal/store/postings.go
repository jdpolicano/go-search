package store

import (
	"database/sql"
)

// upserts a posting
// safety: it is a requirement that the caller handles any errors due to foreign key constraints being broken here.
const insertPostingStmt = `INSERT INTO postings (term_id, doc_id, tf_raw)
VALUES (?, ?, ?)
ON CONFLICT (term_id, doc_id)
DO UPDATE SET
	tf_raw = EXCLUDED.tf_raw;`

type Posting struct {
	TermId int64
	DocId  int64
	TFRaw  int64
}

func NewPosting(termId, docId, tfRaw int64) Posting {
	return Posting{termId, docId, tfRaw}
}

type PostingStore struct {
	db *sql.DB
}

func NewPostingStore(db *sql.DB) *PostingStore {
	return &PostingStore{db}
}

func (ps *PostingStore) InsertPosting(termId, docId, tfRaw int64) error {
	_, err := ps.db.Exec(insertPostingStmt, termId, docId, tfRaw)
	return err
}

func (ps *PostingStore) InsertPostingMany(postings []Posting) error {
	tx, err := ps.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(insertPostingStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, p := range postings {
		if _, err := stmt.Exec(p.TermId, p.DocId, p.TFRaw); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
