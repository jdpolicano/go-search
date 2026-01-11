package store

import "context"

// upserts a posting
// safety: it is a requirement that the caller handles any errors due to foreign key constraints being broken here.
const insertPostingStmt = `INSERT INTO postings (term_id, doc_id, tf_raw)
VALUES ($1, $2, $3)
ON CONFLICT (term_id, doc_id)
DO UPDATE SET
	tf_raw = EXCLUDED.tf_raw;`

const insertPostingsBatchStmt = `INSERT INTO postings (term_id, doc_id, tf_raw)
SELECT t.term_id, t.doc_id, t.tf_raw
FROM unnest($1::int[], $2::int[], $3::int[])
     AS t(term_id, doc_id, tf_raw)
ON CONFLICT (term_id, doc_id)
DO UPDATE SET tf_raw = EXCLUDED.tf_raw;`

type Posting struct {
	TermId int64
	DocId  int64
	TFRaw  int64
}

func NewPosting(termId, docId, tfRaw int64) Posting {
	return Posting{termId, docId, tfRaw}
}

type PostingStore struct {
	db DBTX
}

func NewPostingStore(db DBTX) *PostingStore {
	return &PostingStore{db}
}

func (ps *PostingStore) InsertPosting(ctx context.Context, termId, docId, tfRaw int64) error {
	_, err := ps.db.Exec(ctx, insertPostingStmt, termId, docId, tfRaw)
	return err
}

func (ps *PostingStore) InsertPostingsBatch(ctx context.Context, postings []Posting) error {
	termIds := make([]int64, len(postings))
	docIds := make([]int64, len(postings))
	tfRaws := make([]int64, len(postings))
	for i, p := range postings {
		termIds[i] = p.TermId
		docIds[i] = p.DocId
		tfRaws[i] = p.TFRaw
	}
	_, err := ps.db.Exec(ctx, insertPostingsBatchStmt, termIds, docIds, tfRaws)
	return err
}
