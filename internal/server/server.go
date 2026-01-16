package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jdpolicano/go-search/internal/extract"
	"github.com/jdpolicano/go-search/internal/store"
)

// QueryRequest represents the JSON request for the /query endpoint
type QueryRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit,omitempty"`
}

// QueryResponse represents the JSON response for the /query endpoint
type QueryResponse struct {
	Rankings []store.SearchResult `json:"rankings"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// Server represents the HTTP search server
type Server struct {
	store  store.Store
	logger *slog.Logger
	server *http.Server
}

// NewServer creates a new search server instance
func NewServer(s store.Store, logger *slog.Logger) *Server {
	return &Server{
		store:  s,
		logger: logger,
	}
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/query", s.handleQuery)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/static/", s.handleStatic)

	s.server = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// handleQuery handles the /query POST endpoint
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, http.StatusMethodNotAllowed, "Only POST method is allowed")
		return
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		s.logger.Info("Query processed", "duration", duration, "path", r.URL.Path, "method", r.Method)
	}()

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if req.Query == "" {
		s.sendError(w, http.StatusBadRequest, "Query field is required")
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10 // default limit
	}
	if limit > 100 {
		limit = 100 // max limit
	}

	// Tokenize query using the same scanner as documents
	terms, err := tokenizeQuery(req.Query)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, "Failed to tokenize query: "+err.Error())
		return
	}

	// log user query
	s.logger.Info("User query tokenized", "query", terms)

	// Perform BM25 search
	results, err := store.SearchBM25(r.Context(), s.store.Pool, terms, limit)
	if err != nil {
		s.logger.Error("BM25 search failed", "error", err, "query", req.Query, "terms", terms)
		s.sendError(w, http.StatusInternalServerError, "Search failed")
		return
	}

	response := QueryResponse{
		Rankings: results,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleHealth handles the /health endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleRoot serves the main search interface
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	http.ServeFile(w, r, "assets/web/index.html")
}

// handleStatic serves static files (CSS, JS)
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/static/")

	// Security check: prevent directory traversal
	if strings.Contains(path, "..") || strings.Contains(path, "//") {
		http.NotFound(w, r)
		return
	}

	fullPath := "assets/web/" + path
	http.ServeFile(w, r, fullPath)
}

// sendError sends a JSON error response
func (s *Server) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

// TokenizeQuery uses the same scanner as document processing to tokenize a query
func tokenizeQuery(query string) ([]string, error) {
	if query == "" {
		return nil, errors.New("query cannot be empty")
	}

	terms, err := extract.ScanWordsFromString(query)
	if err != nil {
		return nil, err
	}

	if len(terms) == 0 {
		return nil, errors.New("no valid terms found in query")
	}

	return terms, nil
}
