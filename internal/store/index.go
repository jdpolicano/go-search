// Package store provides indexing and search operations for the search engine.
package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// upsert a doc with a dummy update to get doc_id on conflict
// in future we might want to update title/snippet if they change
const insertDocStmt = `INSERT INTO docs (url, domain, hash, len)
VALUES ($1, $2, $3, $4)
ON CONFLICT (url) DO UPDATE SET
	len = EXCLUDED.len -- just update length on conflict to keep it up to date and ensure we get an id back
RETURNING id;`

// checks if there will be a conflict in docs table based on a hash and domain
const checkDocConflictStmt = `SELECT id FROM docs WHERE domain = $1 AND hash = $2;`

// insert each term frequency for a document, and update term frequencies if they already exist
const insertTermsStmt = `INSERT INTO terms (raw) SELECT unnest($1::text[])
ON CONFLICT (raw) DO UPDATE SET
	raw = EXCLUDED.raw -- dummy update to get id
RETURNING id, raw;
`

// inserts postings, and on unique entries, updates term frequency
const insertPostingsBatchStmt = `INSERT INTO postings (term_id, doc_id, tf_raw)
SELECT t.term_id, $1::int, t.tf_raw -- doc_id is constant for this batch
FROM unnest($2::int[], $3::int[]) AS t(term_id, tf_raw) -- term_id, tf_raw pairs
ON CONFLICT (term_id, doc_id) DO UPDATE
SET tf_raw = EXCLUDED.tf_raw;`

// IndexEntry represents a document ready to be indexed in the search engine.
type IndexEntry struct {
	Url       string         // Original URL
	UrlNorm   string         // Normalized URL for deduplication
	Domain    string         // Domain name
	Hash      string         // Content hash for duplicate detection
	Len       int            // Number of terms in the document
	TermFreqs map[string]int // Term to frequency map for this document
}

// NewIndexEntry creates a new IndexEntry from URL, hash, length, and term frequencies.
func NewIndexEntry(url, hash string, len int, termFreqs map[string]int) (IndexEntry, error) {
	urlNorm, e := NormalizeURL(url)
	if e != nil {
		return IndexEntry{}, e // fallback to raw url if normalization fails
	}

	domain, e := GetHostame(url)
	if e != nil {
		return IndexEntry{}, e
	}

	return IndexEntry{
		Url:       url,
		UrlNorm:   urlNorm,
		Domain:    domain,
		Hash:      hash,
		Len:       len,
		TermFreqs: termFreqs,
	}, nil
}

// IndexDocumentInit performs the initial indexing of a document:
// 1. Inserts document info (url, length) into the docs table.
// 2. Inserts terms into the terms table, getting their term ids.
// 3. Inserts postings into the postings table.
//
// This is only the first phase of the indexing process. There must also be a pre-compute step to calculate TF, IDF, and Norm for terms/docs
// In the database
func IndexDocumentInit(ctx context.Context, db DBTX, doc IndexEntry) error {
	docId, err := insertDocumentInfo(ctx, db, doc.Url, doc.Domain, doc.Hash, doc.Len)
	if err != nil {
		return errors.New("failed to insert document info " + err.Error())
	}

	termIdFreqMap, err := insertTerms(ctx, db, doc.TermFreqs)
	if err != nil {
		return errors.New("failed to insert terms " + err.Error())
	}

	err = insertPostings(ctx, db, docId, termIdFreqMap)
	if err != nil {
		return errors.New("failed to insert postings " + err.Error())
	}

	return nil
}

// insertDocumentInfo inserts a document and returns the id of the document.
// If the document already exists, it returns the existing id, but updates the length.
func insertDocumentInfo(ctx context.Context, db DBTX, url, domain, hash string, len int) (doc_id int64, err error) {
	hasConflict, err := hasDomainHashConflict(ctx, db, domain, hash)
	if err != nil {
		return -1, err
	}

	if hasConflict {
		return -1, errors.New("document with same hash already exists for this domain")
	}

	err = db.QueryRow(ctx, insertDocStmt, url, domain, hash, len).Scan(&doc_id)
	return doc_id, err
}

// hasDomainHashConflict checks if a document with the same hash and domain already exists.
// If it does, it returns true.
func hasDomainHashConflict(ctx context.Context, db DBTX, domain, hash string) (bool, error) {
	var doc_id int64
	err := db.QueryRow(ctx, checkDocConflictStmt, domain, hash).Scan(&doc_id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// insertTerms inserts terms into the term table, returning a map of term_id -> term_frequency for this document.
func insertTerms(ctx context.Context, db DBTX, termFreqs map[string]int) (map[int64]int, error) {
	termIdFreqMap := make(map[int64]int)

	terms := make([]string, 0, len(termFreqs))
	for term := range termFreqs {
		terms = append(terms, term)
	}

	rows, err := db.Query(ctx, insertTermsStmt, terms)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termId int64
		var termRaw string
		if err := rows.Scan(&termId, &termRaw); err != nil {
			return nil, err
		}
		// safety: invariant here is that termFreqs must contain the termRaw key
		// It wouldn't make sense to insert a term that doesn't exist in the term frequency map
		termIdFreqMap[termId] = termFreqs[termRaw]
	}
	return termIdFreqMap, nil
}

// insertPostings inserts postings into the postings table.
func insertPostings(ctx context.Context, db DBTX, docId int64, termIdFreqMap map[int64]int) error {
	termIds := make([]int64, 0, len(termIdFreqMap))
	tfRaws := make([]int64, 0, len(termIdFreqMap))
	for termId, tf := range termIdFreqMap {
		termIds = append(termIds, termId)
		tfRaws = append(tfRaws, int64(tf))
	}
	_, err := db.Exec(ctx, insertPostingsBatchStmt, docId, termIds, tfRaws)
	return err
}
