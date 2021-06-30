package algorithms

import (
	"regexp"
	"strings"
)

type InvertedIndexAlgorithm struct {
	stopWords map[string]string
	invertedIndex InvertedIndex
}

type InvertedIndexEntry struct {
	Term string
	Document []int
}

type InvertedIndex struct {
	HashMap map[string]*InvertedIndexEntry
	Items   []*InvertedIndexEntry
}

func NewInvertedIndexAlgorithm(stopWordList map[string]string) *InvertedIndexAlgorithm {
	return &InvertedIndexAlgorithm{
		stopWords: stopWordList,
		invertedIndex: InvertedIndex{},
	}
}

func (alg *InvertedIndexAlgorithm) Tokenizer(word string) []string {
	var wordList []string
	lowerWord := strings.ToLower(word)
	r := regexp.MustCompile("[^\\s]+")
	wordList = r.FindAllString(lowerWord, -1)
	wordList = RemoveStopWords(wordList, alg.stopWords)

	return wordList
}

func RemoveStopWords(wordList []string, stopWords map[string]string) []string {
	var words []string

	for _, entry := range wordList {
		if stopWords[entry] == "" {
			words = append(words, entry)
		}
	}
	return words
}

func (invertedIndex *InvertedIndex) FindItem(Term string) int {
	for index, item := range invertedIndex.Items {
		if item.Term == Term {
			return index
		}
	}
	panic("Not Found")
}

func (alg *InvertedIndexAlgorithm) CreateInvertedIndex() *InvertedIndex {
	invertedIndex := &InvertedIndex{
		HashMap: make(map[string]*InvertedIndexEntry),
		Items:   []*InvertedIndexEntry{},
	}
	return invertedIndex
}

func (alg *InvertedIndexAlgorithm) AddItem(Term string, Document int, index *InvertedIndex) {
	if index.HashMap[Term] != nil {
		FoundItemPosition := index.FindItem(Term)
		index.Items[FoundItemPosition].Document = append(index.Items[FoundItemPosition].Document, Document)
	} else {
		InvertedIndexEntry := &InvertedIndexEntry{
			Term:            Term,
			Document: []int{Document},
		}
		index.HashMap[Term] = InvertedIndexEntry
		index.Items = append(index.Items, InvertedIndexEntry)
	}
}
