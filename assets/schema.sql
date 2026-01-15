-- Database schema for go-search engine
-- This schema implements an inverted index for full-text search

-- Terms table stores unique terms with their statistical information
-- Used for TF-IDF calculations in search ranking
CREATE TABLE IF NOT EXISTS terms (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  raw TEXT NOT NULL UNIQUE,        -- The actual term/word
  df INTEGER,                     -- Document frequency (how many docs contain this term)
  idf REAL                       -- Inverse document frequency for search ranking
);

-- Documents table stores metadata about crawled web pages
-- Content hash prevents duplicate documents within same domain
CREATE TABLE IF NOT EXISTS docs (
  id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,         -- The full URL to the resource
  domain TEXT NOT NULL,             -- The domain of this document
  hash TEXT NOT NULL,               -- Content hash for duplicate detection
  len INTEGER NOT NULL,             -- Number of terms in the document
  title TEXT,                     -- Optional title for display in search results
  snippet TEXT,                    -- Optional snippet for display in search results
  norm REAL,                       -- Vector magnitude for normalization in TF-IDF
  UNIQUE(domain, hash)              -- Prevent duplicates in same domain
);

-- Postings table implements the inverted index (term -> document mapping)
-- Core of the search engine's indexing structure
CREATE TABLE IF NOT EXISTS postings (
  term_id INTEGER NOT NULL,         -- Foreign key to terms table
  doc_id INTEGER NOT NULL,          -- Foreign key to docs table
  tf_raw INTEGER NOT NULL,          -- Raw term frequency in this document
  PRIMARY KEY (term_id, doc_id),    -- Ensures unique term-doc pairs
  FOREIGN KEY (term_id) REFERENCES terms(id) ON DELETE CASCADE,
  FOREIGN KEY (doc_id) REFERENCES docs(id) ON DELETE CASCADE
);

-- Frontier table manages URLs to be crawled (breadth-first search queue)
-- Tracks crawling state and URL hierarchy
CREATE TABLE IF NOT EXISTS frontier (
  url TEXT PRIMARY KEY,             -- The raw URL
  url_norm TEXT NOT NULL UNIQUE,     -- Normalized URL for deduplication
  parent_url TEXT,                 -- The URL of the parent page (where this link was found)
  depth INTEGER NOT NULL,            -- Depth in the crawling tree
  status INTEGER NOT NULL CHECK(status IN (0, 1, 2, 3)) -- 0: unvisited, 1: in progress, 2: complete, 3: failed
);

-- Performance indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_docs_domain_hash ON docs(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term_id);
CREATE INDEX IF NOT EXISTS idx_postings_doc ON postings(doc_id);
