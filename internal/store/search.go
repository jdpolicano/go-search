package store

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
)

// SearchResult represents a single search result with BM25 score
type SearchResult struct {
	ID      int64   `json:"id"`
	URL     string  `json:"url"`
	Title   *string `json:"title"`
	Snippet *string `json:"snippet"`
	Len     int     `json:"len"`
	Score   float64 `json:"score"`
}

// SearchBM25 performs a BM25 search using the provided query terms
// BM25 parameters: k1=1.2, b=0.75
const searchBM25Stmt = `
WITH
  params AS (
    SELECT 1.2::real AS k1, 0.75::real AS b
  ),
  corpus AS (
    SELECT COUNT(*)::real AS N, AVG(len)::real AS avgdl
    FROM docs
    WHERE len > 0
  ),
  q AS (
    -- de-dupe query terms (BM25 typically doesn't need query TF for basic ranking)
    SELECT DISTINCT UNNEST($1::text[]) AS raw
  )
SELECT
  d.id,
  d.url,
  d.title,
  d.snippet,
  d.len,
  SUM(
    -- idf (BM25 variant; +1 makes it non-negative even for very common terms)
    (LN(((corpus.N - t.df::real + 0.5) / (t.df::real + 0.5)) + 1.0))
    *
    -- BM25 tf component with length normalization
    (
      (p.tf_raw::real * (params.k1 + 1.0))
      /
      (p.tf_raw::real
        + params.k1 * (1.0 - params.b + params.b * (d.len::real / NULLIF(corpus.avgdl, 0)))
      )
    )
  ) AS score
FROM q
JOIN terms t     ON t.raw = q.raw
JOIN postings p  ON p.term_id = t.id
JOIN docs d      ON d.id = p.doc_id
CROSS JOIN params
CROSS JOIN corpus
WHERE d.len > 0
  AND t.df IS NOT NULL
GROUP BY d.id, d.url, d.title, d.snippet, d.len
HAVING COUNT(DISTINCT t.raw) >= $2
ORDER BY score DESC
LIMIT $3;`

func SearchBM25(ctx context.Context, db DBTX, terms []string, limit int) ([]SearchResult, error) {
	if len(terms) == 0 {
		return nil, errors.New("no terms provided for search")
	}

	if limit <= 0 {
		limit = 10 // default limit
	}

	rows, err := db.Query(ctx, searchBM25Stmt, terms, min(len(terms), 2), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var result SearchResult
		err := rows.Scan(
			&result.ID,
			&result.URL,
			&result.Title,
			&result.Snippet,
			&result.Len,
			&result.Score,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// SearchResultSlice is a helper type for JSON marshaling
type SearchResultSlice []SearchResult

// Value implements the driver.Valuer interface for database compatibility
func (s SearchResultSlice) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Scan implements the sql.Scanner interface for database compatibility
func (s *SearchResultSlice) Scan(value interface{}) error {
	if value == nil {
		*s = SearchResultSlice{}
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, s)
	case string:
		return json.Unmarshal([]byte(v), s)
	default:
		return errors.New("cannot scan into SearchResultSlice")
	}
}
