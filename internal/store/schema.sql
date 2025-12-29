CREATE TABLE IF NOT EXISTS terms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  raw TEXT NOT NULL UNIQUE,
  df INTEGER NOT NULL,
  idf REAL
);

CREATE TABLE IF NOT EXISTS docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT NOT NULL UNIQUE,
  title TEXT,
  snippet TEXT,
  len INTEGER NOT NULL, -- number of terms in the document
  norm REAL -- magnitude for normalization
);

CREATE TABLE IF NOT EXISTS postings (
  term_id INTEGER NOT NULL,
  doc_id INTEGER NOT NULL,
  tf_raw INTEGER NOT NULL,
  PRIMARY KEY (term_id, doc_id),
  FOREIGN KEY (term_id) REFERENCES terms(term_id) ON DELETE CASCADE,
  FOREIGN KEY (doc_id) REFERENCES docs(doc_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS frontier (
  url TEXT PRIMARY KEY, -- the raw URL
  url_norm TEXT NOT NULL UNIQUE, -- normalized URL for comparison
  parent_url TEXT, -- the URL of the parent page
  depth INTEGER NOT NULL, -- depth in the crawl
  status INTEGER NOT NULL CHECK(status IN (0, 1, 2, 3)) -- 0: unvisited, 1: in progress, 2: complete 3: failed
);

CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term_id);
CREATE INDEX IF NOT EXISTS idx_postings_doc ON postings(doc_id);
