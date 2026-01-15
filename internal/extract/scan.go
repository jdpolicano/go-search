// Package extract provides text scanning and word processing utilities.
package extract

import (
	"bufio"
	"bytes"
	_ "embed"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

//go:embed stop_words.txt
var stopWordsData string
var stopWords = initStopWords()

// initStopWords initializes the stop words map from the embedded file.
func initStopWords() map[string]any {
	lines := strings.Split(stopWordsData, "\n")
	stopWords := make(map[string]any, len(lines))
	for _, line := range lines {
		word := strings.TrimSpace(line)
		if word != "" {
			stopWords[word] = nil
		}
	}
	return stopWords
}

// isAlphaNumericRune checks if a rune is a letter or number.
func isAlphaNumericRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

// ScanAlphaNumericWord is a bufio.SplitFunc that scans for alphanumeric words.
// It skips non-alphanumeric characters and returns the next word in lowercase.
func ScanAlphaNumericWord(data []byte, isEof bool) (int, []byte, error) {
	start := 0
	// Skip anything that isn't alphanumeric to begin.
	for start < len(data) {
		r, size := utf8.DecodeRune(data[start:])
		if isAlphaNumericRune(r) {
			break
		}
		start += size
	}

	end := start
	for end < len(data) {
		r, size := utf8.DecodeRune(data[end:])
		// We've reached the end of our sequence
		if !isAlphaNumericRune(r) {
			return end + size, bytes.ToLower(data[start:end]), nil
		}
		end += size
	}

	// There were alphanumeric runes
	if start < len(data) {
		return end, bytes.ToLower(data[start:end]), nil
	}

	// Entire string was non-alphanumeric
	return end, nil, nil
}

// ScanWords scans text from an io.Reader and returns filtered words.
// It removes stop words and integer words, returning lowercase results.
func ScanWords(reader io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Split(ScanAlphaNumericWord)

	words := make([]string, 0, 1024)
	for scanner.Scan() {
		word := scanner.Text()
		if _, isStopWord := stopWords[word]; !isStopWord && !isIntegerWord(word) {
			words = append(words, strings.ToLower(word))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return words, nil
}

// ScanWordsFromString scans text from a string and returns filtered words.
func ScanWordsFromString(s string) ([]string, error) {
	return ScanWords(strings.NewReader(s))
}

// isIntegerWord checks if a word represents an integer value.
func isIntegerWord(w string) bool {
	_, err := strconv.Atoi(w)
	return err == nil
}
