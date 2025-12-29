package store

import (
	"database/sql"
	_ "embed"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schemaSQL string

type Store struct {
	db *sql.DB
}

func NewStore(dbPath string) (*Store, error) {
	fileUri := fmt.Sprintf("file:%s?_fk=on&cache=shared&mode=rwc&_busy_timeout=5000&_journal_mode=WAL", dbPath)
	db, openErr := sql.Open("sqlite3", fileUri)
	if openErr != nil {
		return nil, openErr
	}

	if _, execErr := db.Exec(schemaSQL); execErr != nil {
		return nil, execErr
	}

	db.SetMaxOpenConns(1)
	return &Store{db}, nil
}

func (s *Store) IntoFrontierStore() *FrontierStore {
	return NewFrontierStore(s.db)
}

func (s *Store) IntoTermStore() *TermStore {
	return NewTermStore(s.db)
}

func (s *Store) IntoDocumentStore() *DocStore {
	return NewDocStore(s.db)
}

func (s *Store) IntoPostingStore() *PostingStore {
	return NewPostingStore(s.db)
}

func (s *Store) Close() error {
	return s.db.Close()
}
