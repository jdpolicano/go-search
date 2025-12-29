package store

import (
	"database/sql"
	"fmt"
	"strings"
)

// Insert a new term, or do nothing if it already exists.
// The "do nothing" is implemented as an update that sets the term_raw to itself.
// This allows us to use the RETURNING clause to get the term_id in both cases.
const InsertDocTerms = `INSERT INTO terms (raw, df) VALUES (?, 1)
ON CONFLICT(raw)
	DO UPDATE SET df = df + 1
RETURNING id;`

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
// Duplicates in the input slice are ignored.
// This function should only be called once per document as reflected in the "docs" table
// to avoid over incrementing the document frequency (df) of terms.
func (ts *TermStore) InsertDocTerms(terms []string) (TermStats, error) {
	termIds := make(map[string]int64, len(terms))
	termFreqs := make(map[string]int64, len(terms))
	tx, err := ts.db.Begin()
	if err != nil {
		return TermStats{}, err
	}
	stmt, err := tx.Prepare(InsertDocTerms)
	if err != nil {
		return TermStats{}, err
	}
	defer stmt.Close()
	for _, term := range terms {
		termFreqs[term]++
		// we can make sure to not insert duplicates in this batch
		if _, exists := termIds[term]; exists {
			continue
		}
		var termId int64
		if err := stmt.QueryRow(term).Scan(&termId); err != nil {
			tx.Rollback()
			return TermStats{}, err
		}
		termIds[term] = termId
	}
	return TermStats{termIds, termFreqs}, tx.Commit()
}

func (ts *TermStore) GetTermIds(terms map[string]int64) error {
	placeholders := make([]string, 0, len(terms))
	args := make([]any, 0, len(terms))
	for word, _ := range terms {
		placeholders = append(placeholders, "?")
		args = append(args, word)
	}
	placeHolderStr := strings.Join(placeholders, ", ")
	query := fmt.Sprintf("SELECT term_id, term_raw FROM terms WHERE term_raw IN (%s)", placeHolderStr)
	rows, err := ts.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var termId int64
		var termRaw string
		if err := rows.Scan(&termId, &termRaw); err != nil {
			return err
		}
		terms[termRaw] = termId
	}
	return nil
}
