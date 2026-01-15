CREATE TABLE IF NOT EXISTS terms (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  raw TEXT NOT NULL UNIQUE,
  df INTEGER, -- document frequency
  idf REAL -- inverse document frequency
);

CREATE TABLE IF NOT EXISTS docs (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  url TEXT NOT NULL UNIQUE, -- The full url to the resource
  domain TEXT NOT NULL, -- The domain of this document
  hash TEXT NOT NULL, -- The hash of the text content at the time this doc was scraped
  len INTEGER NOT NULL, -- number of terms in the document
  title TEXT, -- optional title to show pretty output to users
  snippet TEXT, -- optional snippet to show pretty output to users
  norm REAL, -- magnitude (vector length) for normalization
  UNIQUE(domain, hash)
);

CREATE TABLE IF NOT EXISTS postings (
  term_id INTEGER NOT NULL,
  doc_id INTEGER NOT NULL,
  tf_raw INTEGER NOT NULL,
  PRIMARY KEY (term_id, doc_id),
  FOREIGN KEY (term_id) REFERENCES terms(id) ON DELETE CASCADE,
  FOREIGN KEY (doc_id) REFERENCES docs(id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS frontier (
  url TEXT PRIMARY KEY, -- the raw URL
  url_norm TEXT NOT NULL UNIQUE, -- normalized URL for comparison
  parent_url TEXT, -- the URL of the parent page
  depth INTEGER NOT NULL, -- depth in the crawl
  status INTEGER NOT NULL CHECK(status IN (0, 1, 2, 3)) -- 0: unvisited, 1: in progress, 2: complete 3: failed
);

CREATE INDEX IF NOT EXISTS idx_docs_domain_hash ON docs(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term_id);
CREATE INDEX IF NOT EXISTS idx_postings_doc ON postings(doc_id);
