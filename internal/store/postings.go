package store

import (
	"database/sql"
	"strings"
)

type Posting struct {
	TermId int
	DocId  int
	TFRaw  int
}

func NewPosting(termId, docId, tfRaw int) Posting {
	return Posting{termId, docId, tfRaw}
}

type PostingStore struct {
	db *sql.DB
}

func NewPostingStore(db *sql.DB) *PostingStore {
	return &PostingStore{db}
}

func (ps *PostingStore) GetByTermId(termId int) ([]Posting, error) {
	rows, err := ps.db.Query("SELECT term_id, doc_id, tf_raw FROM postings WHERE term_id = ?", termId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	postings := make([]Posting, 0)
	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.TermId, &p.DocId, &p.TFRaw); err != nil {
			return nil, err
		}
		postings = append(postings, p)
	}
	return postings, nil
}

func (ps *PostingStore) GetByTermIds(termIds []int) ([]Posting, error) {
	if len(termIds) == 0 {
		return []Posting{}, nil
	}
	placeholders := make([]string, len(termIds))
	args := make([]any, len(termIds))
	for i, _ := range termIds {
		placeholders[i] = "?"
		args[i] = termIds[i]
	}
	placeHolderStr := strings.Join(placeholders, ", ")
	query := "SELECT term_id, doc_id, tf_raw FROM postings WHERE term_id IN (" + placeHolderStr + ")"
	rows, err := ps.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	postings := make([]Posting, 0)
	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.TermId, &p.DocId, &p.TFRaw); err != nil {
			return nil, err
		}
		postings = append(postings, p)
	}
	return postings, nil
}

func (ps *PostingStore) GetByDocId(docId int) ([]Posting, error) {
	rows, err := ps.db.Query("SELECT term_id, doc_id, tf_raw FROM postings WHERE doc_id = ?", docId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	postings := make([]Posting, 0)
	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.TermId, &p.DocId, &p.TFRaw); err != nil {
			return nil, err
		}
		postings = append(postings, p)
	}
	return postings, nil
}

func (ps *PostingStore) Insert(p Posting) error {
	_, err := ps.db.Exec("INSERT INTO postings (term_id, doc_id, tf_raw) VALUES (?, ?, ?)", p.TermId, p.DocId, p.TFRaw)
	return err
}

func (ps *PostingStore) InsertMany(postings []Posting) error {
	tx, err := ps.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO postings (term_id, doc_id, tf_raw) VALUES (?, ?, ?)")
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
