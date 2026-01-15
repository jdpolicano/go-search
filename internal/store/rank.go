package store

import (
	"context"
)

// UpdateDocumentFrequency updates the df (document frequency) for all terms
// based on the current postings. Phase 1 of the ranking update process.
const updateDocumentFrequencyStmt = `UPDATE terms t
SET df = x.df
FROM (
  SELECT term_id, COUNT(*)::int AS df
  FROM postings
  GROUP BY term_id
) x
WHERE t.id = x.term_id;`

// SetZeroDfForTermsWithNoPostings ensures terms with no postings get df=0
const setZeroDfForTermsWithNoPostingsStmt = `UPDATE terms SET df = 0 WHERE df IS NULL;`

func UpdateDocumentFrequency(ctx context.Context, db DBTX) error {
	_, err := db.Exec(ctx, updateDocumentFrequencyStmt)
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, setZeroDfForTermsWithNoPostingsStmt)
	return err
}

// UpdateInverseDocumentFrequency updates the idf for all terms using
// smoothed IDF formula: ln((N + 1)/(df + 1)) + 1
// Phase 2 of the ranking update process.
const updateInverseDocumentFrequencyStmt = `WITH n AS (
  SELECT COUNT(*)::real AS N FROM docs
)
UPDATE terms t
SET idf = LN((n.N + 1.0) / (t.df + 1.0)) + 1.0
FROM n;`

func UpdateInverseDocumentFrequency(ctx context.Context, db DBTX) error {
	_, err := db.Exec(ctx, updateInverseDocumentFrequencyStmt)
	return err
}

// UpdateDocumentNorms updates the norm (vector magnitude) for all documents
// using TF-IDF weights. Phase 3 of the ranking update process.
// TF formula: 1 + ln(tf_raw)
// Norm formula: sqrt(sum((tf * idf)^2))
const updateDocumentNormsStmt = `UPDATE docs d
SET norm = x.norm
FROM (
  SELECT
    p.doc_id,
    SQRT(SUM(POWER((1.0 + LN(p.tf_raw::real)) * t.idf, 2))) AS norm
  FROM postings p
  JOIN terms t ON t.id = p.term_id
  GROUP BY p.doc_id
) x
WHERE d.id = x.doc_id;`

// SetZeroNormForDocsWithNoPostings ensures docs with no postings get norm=0
const setZeroNormForDocsWithNoPostingsStmt = `UPDATE docs SET norm = 0 WHERE norm IS NULL;`

func UpdateDocumentNorms(ctx context.Context, db DBTX) error {
	_, err := db.Exec(ctx, updateDocumentNormsStmt)
	if err != nil {
		return err
	}

	_, err = db.Exec(ctx, setZeroNormForDocsWithNoPostingsStmt)
	return err
}
