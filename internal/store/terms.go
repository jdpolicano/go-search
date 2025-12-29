package store

import (
	"database/sql"
	"fmt"
	"strings"
)

// Insert a new term, guarrenteed to return an id.
const insertTermIncDfStmt = `INSERT INTO terms (raw) VALUES (?)
ON CONFLICT(raw) DO UPDATE SET
	df = terms.df + 1
RETURNING id;`

// Update the df for a certain term
const updateDFStmt = `UPDATE terms
SET df = df + 1
WHERE id = (?);`

// Update idf for a certain term
const updateIDFStmt = `UPDATE terms
SET idf = (?)
WHERE id = (?);`

type TermItem struct {
	TermId  int
	TermRaw string
	DF      sql.NullInt64
	IDF     sql.NullFloat64
}

type TermStats struct {
	IDs map[string]int64
	TF  map[string]int64
}

func NewTermStats() TermStats {
	return TermStats{make(map[string]int64), make(map[string]int64)}
}

func (ts TermStats) UpsertTF(word string) {
	if cnt, exists := ts.TF[word]; exists {
		ts.TF[word] = cnt + 1
	} else {
		ts.TF[word] = 1
	}
}

func (ts TermStats) AddId(word string, id int64) {
	ts.IDs[word] = id
}

func (ts TermStats) HasTermId(word string) bool {
	_, exists := ts.IDs[word]
	return exists
}


func (ts TermStats) IntoPostings(docId int64) []Posting {
	postings := make([]Posting, 0, len(ts.IDs))
	for term, id := range ts.IDs {
		freq := ts.TF[term]
		posting := Posting{id, docId, freq}
		postings = append(postings, posting)
	}
	return postings
}

type TermStore struct {
	db *sql.DB
}

func NewTermStore(db *sql.DB) *TermStore {
	return &TermStore{db}
}

func (ts *TermStore) GetByTermId(termId int) (*TermItem, error) {
	var ti TermItem
	row := ts.db.QueryRow("SELECT id, raw, df, idf FROM terms WHERE term_id = ?", termId)
	err := row.Scan(&ti.TermId, &ti.TermRaw, &ti.DF, &ti.IDF)
	return &ti, err
}

func (ts *TermStore) GetByTermIds(termIds []int) ([]*TermItem, error) {
	if len(termIds) == 0 {
		return []*TermItem{}, nil
	}

	placeholders := make([]string, len(termIds))
	args := make([]any, len(termIds))
	for i, _ := range termIds {
		placeholders[i] = "?"
		args[i] = termIds[i]
	}

	placeHolderStr := strings.Join(placeholders, ", ")
	query := fmt.Sprintf("SELECT term_id, term_raw, df FROM terms WHERE term_id IN (%s)", placeHolderStr)
	rows, err := ts.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	terms := make([]*TermItem, 0, len(termIds))
	for rows.Next() {
		var ti TermItem
		if err := rows.Scan(&ti.TermId, &ti.TermRaw, &ti.DF); err != nil {
			return nil, err
		}

		terms = append(terms, &ti)
	}

	return terms, nil
}

func (ts *TermStore) GetByTermRaw(termRaw string) (*TermItem, error) {
	var ti TermItem
	row := ts.db.QueryRow("SELECT term_id, term_raw, df FROM terms WHERE term_raw = ?", termRaw)
	err := row.Scan(&ti.TermId, &ti.TermRaw, &ti.DF)
	return &ti, err
}

func (ts *TermStore) GetByTermsRaw(termRaws []string) ([]TermItem, error) {
	if len(termRaws) == 0 {
		return []TermItem{}, nil
	}

	placeholders := make([]string, len(termRaws))
	args := make([]any, len(termRaws))
	for i, _ := range termRaws {
		placeholders[i] = "?"
		args[i] = termRaws[i]
	}

	placeHolderStr := strings.Join(placeholders, ", ")
	query := fmt.Sprintf("SELECT term_id, term_raw, df FROM terms WHERE term_raw IN (%s)", placeHolderStr)
	rows, err := ts.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	terms := make([]TermItem, 0, len(termRaws))
	for rows.Next() {
		var ti TermItem
		if err := rows.Scan(&ti.TermId, &ti.TermRaw, &ti.DF); err != nil {
			return nil, err
		}

		terms = append(terms, ti)
	}

	return terms, rows.Err()
}

// Inserts multiple terms in a single transaction returning a map of term raw to term id.
// Duplicates in the input slice are ignored. This query also increments the document frequency for each unique term.
// It is the requirment of the caller to ensure th
func (ts *TermStore) InsertTermsIncDf(terms []string) (TermStats, error) {
	stats := NewTermStats()
	tx, err := ts.db.Begin()
	if err != nil {
		return TermStats{}, err
	}
	stmt, err := tx.Prepare(insertTermIncDfStmt)
	if err != nil {
		return TermStats{}, err
	}
	defer stmt.Close()
	for _, term := range terms {
		// first update the terms frequency counter
		stats.UpsertTF(term)

		// if we already inserted this, no need to increment the df in "term" table again
		if stats.HasTermId(term) {
			continue
		}

		// insert the term and record the terms id in the db.
		var termId int64
		if err := stmt.QueryRow(term).Scan(&termId); err != nil {
			tx.Rollback()
			return TermStats{}, err
		}
		stats.AddId(term, termId)
	}

	return stats, tx.Commit()
}

func (ts *TermStore) IncrementDFMany(termIds map[string]int64) error {
	tx, err := ts.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(updateDFStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, id := range termIds {
		if _, err := stmt.Exec(id); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (ts *TermStore) IncrementIDF(termIDFs map[int64]float64) error {
	tx, err := ts.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(updateIDFStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for id, idf := range termIDFs {
		if _, err := stmt.Exec(idf, id); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
