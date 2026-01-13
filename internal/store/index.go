package store

import (
	"context"
	"errors"
)

// upsert a doc with a dummy update to get doc_id on conflict
// in the future we might want to update title/snippet if they change
const insertDocStmt = `INSERT INTO docs (url, len)
VALUES ($1, $2)
ON CONFLICT (url) DO UPDATE SET
	len = EXCLUDED.len -- just update length on conflict to keep it up to date and ensure we get an id back
RETURNING id;`

// insert each term frequency for a document, and update the term frequencies if they already exist
const insertTermsStmt = `INSERT INTO terms (raw) SELECT unnest($1::text[])
ON CONFLICT (raw) DO UPDATE SET
	raw = EXCLUDED.raw -- dummy update to get the id
RETURNING id, raw;
`

// inserts the postings, and on unique entries, updates the term frequency
const insertPostingsBatchStmt = `INSERT INTO postings (term_id, doc_id, tf_raw)
SELECT t.term_id, $1::int, t.tf_raw -- doc_id is constant for this batch
FROM unnest($2::int[], $3::int[]) AS t(term_id, tf_raw) -- term_id, tf_raw pairs
ON CONFLICT (term_id, doc_id) DO UPDATE
SET tf_raw = EXCLUDED.tf_raw;`

type IndexEntry struct {
	Url       string
	UrlNorm   string
	Len       int
	TermFreqs map[string]int // term -> frequency map
}

func NewIndexEntry(url, url_norm string, len int, termFreqs map[string]int) IndexEntry {
	return IndexEntry{url, url_norm, len, termFreqs}
}

/*
 * Performs the initial indexing of a document:
 * 1. Inserts the document info (url, length) into the docs table.
 * 2. Inserts the terms into the terms table, getting their term ids.
 * 3. Inserts the postings into the postings table.
 *
 * This is only the first phase of the indexing process, there must also be a pre-compute step to calculate the TF, IDF, and Norm for the terms/docs
 * In the database
 */
func IndexDocumentInit(ctx context.Context, db DBTX, doc IndexEntry) error {
	docId, err := insertDocumentInfo(ctx, db, doc.Url, doc.Len)
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

/**
 * Inserts a document and returns the id of the document.
 * If the document already exists, it returns the existing id, but updates the length.
 */
func insertDocumentInfo(ctx context.Context, db DBTX, url string, len int) (int64, error) {
	var doc_id int64
	err := db.QueryRow(ctx, insertDocStmt, url, len).Scan(&doc_id)
	return doc_id, err
}

/**
 * Inserts terms into the term table, returning a map of term_id -> term_frequency for this document.
 */
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
		// safety: the invariant here is that termFreqs must contain the termRaw key
		// It wouldn't make sense to insert a term that doesn't exist in the term frequency map
		termIdFreqMap[termId] = termFreqs[termRaw]
	}
	return termIdFreqMap, nil
}

/**
 * Inserts postings into the postings table.
 */
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
