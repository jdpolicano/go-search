package document

// import (
// 	"bufio"
// 	"bytes"
// 	"io"
// 	"math"
// 	"unicode/utf8"
// )

// type Scanner interface {
// 	Scan() bool
// 	Token() string
// 	Err() error
// }

// type TextDocumentScanner struct {
// 	scanner *bufio.Scanner
// 	hasMore bool
// 	token   string
// }

// func NewTextDocumentScanner(r io.Reader) *TextDocumentScanner {
// 	scanner := bufio.NewScanner(r)
// 	scanner.Split(scanAlphaNumericWord)
// 	hasMore := true
// 	token := ""
// 	return &TextDocumentScanner{
// 		scanner,
// 		hasMore,
// 		token,
// 	}
// }

// func (tds *TextDocumentScanner) Scan() bool {
// 	if tds.hasMore {
// 		tds.hasMore = tds.scanner.Scan()
// 		tds.token = tds.scanner.Text()
// 	}
// 	return tds.hasMore
// }

// func (tds *TextDocumentScanner) Token() string {
// 	return tds.token
// }

// func (tds *TextDocumentScanner) Err() error {
// 	return tds.scanner.Err()
// }

// type Vector struct {
// 	// the number of unique terms
// 	size int
// 	// term frequencies inverse doc frequencies
// 	tfidf map[string]float64
// 	// the vector's magnitude/length
// 	magnitude float64
// }

// func NewVector(d *Document, idfMap map[string]float64, allocSize int) (Vector, error) {
// 	tfidf := make(map[string]float64, allocSize)
// 	numTerms := float64(d.Length)
// 	for word, cnt := range d.Terms {
// 		if idf, exists := idfMap[word]; exists {
// 			tf := float64(cnt) / numTerms
// 			tfidf[word] = tf * idf
// 		}
// 	}

// 	magnitude := float64(0)
// 	for _, tfidfVal := range tfidf {
// 		magnitude += math.Pow(tfidfVal, 2) // magnitude is the square of all vector values "rooted"
// 	}
// 	magnitude = math.Sqrt(magnitude)

// 	return Vector{
// 		size:      len(tfidf),
// 		tfidf:     tfidf,
// 		magnitude: magnitude,
// 	}, nil
// }

// func (v Vector) CosineSimilarity(other Vector) float64 {
// 	// swap to the smaller of the two to reduce iterations
// 	if other.size > v.size {
// 		other, v = v, other
// 	}
// 	sum := 0.0
// 	for word, otherTfidf := range other.tfidf {
// 		if vTfidf, exists := v.tfidf[word]; exists {
// 			sum += otherTfidf * vTfidf
// 		}
// 	}
// 	denominator := v.magnitude * other.magnitude
// 	if denominator == 0 {
// 		return denominator
// 	}
// 	return sum / denominator
// }

// type Document struct {
// 	Path   string
// 	Length int
// 	Terms  map[string]int
// }

// func NewDocument(name string, stopWords map[string]any, r io.Reader) (*Document, error) {
// 	scanner := NewTextDocumentScanner(r)
// 	terms := make(map[string]int)
// 	length := 0
// 	scanErr := ForEachWord(scanner, func(w string) {
// 		if _, shouldStop := stopWords[w]; shouldStop {
// 			return
// 		}
// 		length += 1
// 		if cnt, exists := terms[w]; exists {
// 			terms[w] = cnt + 1
// 		} else {
// 			terms[w] = 1
// 		}
// 	})
// 	if scanErr != nil {
// 		return nil, scanErr
// 	}
// 	return &Document{
// 		name,
// 		length,
// 		terms,
// 	}, nil
// }

// func GetIdfMap(documents []*Document) map[string]float64 {
// 	idfMap := make(map[string]float64, 16384) // 2^14 words to start
// 	for _, doc := range documents {
// 		for word, _ := range doc.Terms {
// 			if cnt, exists := idfMap[word]; exists {
// 				idfMap[word] = cnt + 1
// 			} else {
// 				idfMap[word] = 1
// 			}
// 		}
// 	}
// 	numDocs := float64(len(documents))
// 	for word, cnt := range idfMap {
// 		idfMap[word] = math.Log(1 + (numDocs / cnt))
// 	}
// 	return idfMap
// }

// func ForEachWord(s Scanner, cb func(w string)) error {
// 	for s.Scan() {
// 		word := s.Token()
// 		cb(word)
// 	}

// 	if err := s.Err(); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func scanAlphaNumericWord(data []byte, isEof bool) (int, []byte, error) {
// 	start := 0
// 	// skip anything that isn't alphanumeric to begin.
// 	for start < len(data) {
// 		r, size := utf8.DecodeRune(data[start:])
// 		if isAlphaNumericRune(r) {
// 			break
// 		}
// 		start += size
// 	}

// 	end := start
// 	for end < len(data) {
// 		r, size := utf8.DecodeRune(data[end:])
// 		// we've reached the end of our sequence
// 		if !isAlphaNumericRune(r) {
// 			return end + size, bytes.ToLower(data[start:end]), nil
// 		}
// 		end += size
// 	}

// 	// there were alphanum runes
// 	if start < len(data) {
// 		return end, bytes.ToLower(data[start:]), nil
// 	}

// 	// entire string was non-sense
// 	return end, nil, nil
// }
