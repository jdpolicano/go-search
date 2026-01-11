package store

import (
	"context"
)

// Insert a new term, guarrenteed to return an id.
const insertTermIncDfStmt = `INSERT INTO terms (raw) VALUES ($1)
ON CONFLICT(raw) DO UPDATE SET
	df = df + 1
RETURNING id;`

type TermStore struct {
	db DBTX
}

func NewTermStore(db DBTX) *TermStore {
	return &TermStore{db}
}

func (ts *TermStore) GetRawByTermId(ctx context.Context, termId int) (string, error) {
	var raw string
	row := ts.db.QueryRow(ctx, "SELECT raw FROM terms WHERE id = $1", termId)
	err := row.Scan(&raw)
	return raw, err
}

func (ts *TermStore) GetByTermIds(ctx context.Context, termIds []int) ([]*TermItem, error) {
	if len(termIds) == 0 {
		return []*TermItem{}, nil
	}

	rows, err := ts.db.Query(ctx, "SELECT id, raw FROM terms WHERE id = ANY(%1)", termIds)
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

func (ts *TermStore) GetIdByRaw(ctx context.Context, termRaw string) (int64, error) {
	var id int64
	row := ts.db.QueryRow(ctx, "SELECT id, raw FROM terms WHERE raw = $1", termRaw)
	err := row.Scan(&id)
	return id, err
}

func (ts *TermStore) GetIdsByRaws(ctx context.Context, termRaws []string) (map[string]int64, error) {
	if len(termRaws) == 0 {
		return map[string]int64{}, nil
	}

	rows, err := ts.db.Query(ctx, "SELECT id, raw FROM terms WHERE raw = ANY(%1)", termRaws)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	terms := make(map[string]int64, len(termRaws))
	for rows.Next() {
		var id int64
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		terms[raw] = id
	}

	return terms, rows.Err()
}

func (ts *TermStore) GetIDFsFromRaws(ctx context.Context, termRaws []string) (map[string]float64, error) {
	if len(termRaws) == 0 {
		return map[string]float64{}, nil
	}

	idsMap, err := ts.GetIdsByRaws(ctx, termRaws)
	if err != nil {
		return nil, err
	}
}
