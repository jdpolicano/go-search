package language

type Language int

/**
 * Supported languages, english with correleate with iso codes EN and ENG for example.
 */
const (
	English Language = iota
)

type IsoCode639_1 int

/**
 * Package language provides utilities for language detection.
 * These are the iso 639-1 language codes supported.
 */
const (
	EN IsoCode639_1 = iota // "en"
)

type IsoCode639_3 int

/**
 * Package language provides utilities for language detection.
 * These are the iso 639-3 language codes supported.
 */
const (
	ENG IsoCode639_3 = iota // "eng"
)

func (iso1 IsoCode639_1) String() string {
	switch iso1 {
	case EN:
		return "en"
	default:
		return ""
	}
}

func (iso3 IsoCode639_3) String() string {
	switch iso3 {
	case ENG:
		return "eng"
	default:
		return ""
	}
}

func GetLanguageFromIsoCode639_1(iso1 IsoCode639_1) Language {
	switch iso1 {
	case EN:
		return English
	default:
		return -1
	}
}

func GetLanguageFromIsoCode639_3(iso3 IsoCode639_3) Language {
	switch iso3 {
	case ENG:
		return English
	default:
		return -1
	}
}

func GetIsoCode639_1FromValue(val string) IsoCode639_1 {
	switch val {
	case "en":
		return EN
	default:
		return -1
	}
}

func GetIsoCode639_3FromValue(val string) IsoCode639_3 {
	switch val {
	case "eng":
		return ENG
	default:
		return -1
	}
}
