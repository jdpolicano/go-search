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

func isAlphaNumericRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

func ScanAlphaNumericWord(data []byte, isEof bool) (int, []byte, error) {
	start := 0
	// skip anything that isn't alphanumeric to begin.
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
		// we've reached the end of our sequence
		if !isAlphaNumericRune(r) {
			return end + size, bytes.ToLower(data[start:end]), nil
		}
		end += size
	}

	// there were alphanum runes
	if start < len(data) {
		return end, bytes.ToLower(data[start:]), nil
	}

	// entire string was non-sense
	return end, nil, nil
}

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


func isIntegerWord(w string) bool {
	_, err := strconv.Atoi(w)
	return err == nil
}
