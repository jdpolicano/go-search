// Package language provides language enumeration and ISO code utilities for the search engine.
package language

// Language represents supported languages for content processing.
type Language int

// Supported languages, English correlates with ISO codes EN and ENG.
const (
	English Language = iota
)

// IsoCode639_1 represents ISO 639-1 two-letter language codes.
type IsoCode639_1 int

// ISO 639-1 language codes supported by the search engine.
const (
	EN IsoCode639_1 = iota // "en" - English
)

// IsoCode639_3 represents ISO 639-3 three-letter language codes.
type IsoCode639_3 int

// ISO 639-3 language codes supported by the search engine.
const (
	ENG IsoCode639_3 = iota // "eng" - English
)

// String returns the string representation of ISO 639-1 language codes.
func (iso1 IsoCode639_1) String() string {
	switch iso1 {
	case EN:
		return "en"
	default:
		return ""
	}
}

// String returns the string representation of ISO 639-3 language codes.
func (iso3 IsoCode639_3) String() string {
	switch iso3 {
	case ENG:
		return "eng"
	default:
		return ""
	}
}

// GetLanguageFromIsoCode639_1 converts ISO 639-1 code to Language enum.
func GetLanguageFromIsoCode639_1(iso1 IsoCode639_1) Language {
	switch iso1 {
	case EN:
		return English
	default:
		return -1
	}
}

// GetLanguageFromIsoCode639_3 converts ISO 639-3 code to Language enum.
func GetLanguageFromIsoCode639_3(iso3 IsoCode639_3) Language {
	switch iso3 {
	case ENG:
		return English
	default:
		return -1
	}
}

// GetIsoCode639_1FromValue converts string value to ISO 639-1 code.
func GetIsoCode639_1FromValue(val string) IsoCode639_1 {
	switch val {
	case "en":
		return EN
	default:
		return -1
	}
}

// GetIsoCode639_3FromValue converts string value to ISO 639-3 code.
func GetIsoCode639_3FromValue(val string) IsoCode639_3 {
	switch val {
	case "eng":
		return ENG
	default:
		return -1
	}
}
