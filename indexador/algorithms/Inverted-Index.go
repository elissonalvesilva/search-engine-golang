package algorithms

import (
	"strings"
)

type InvertedIndexAlgorithm struct {
	stopWords     map[string]string
	invertedIndex InvertedIndex
}

type InvertedIndexEntry struct {
	Term     string
	Index    int
	Document []int
}

type InvertedIndex struct {
	HashMap map[string]*InvertedIndexEntry
	Items   []*InvertedIndexEntry
}

func NewInvertedIndexAlgorithm(stopWordList map[string]string) *InvertedIndexAlgorithm {
	return &InvertedIndexAlgorithm{
		stopWords:     stopWordList,
		invertedIndex: InvertedIndex{},
	}
}

func (alg *InvertedIndexAlgorithm) Tokenizer(word string) []string {
	var wordList []string
	lowerWord := strings.ToLower(word)
	wordList = strings.Split(lowerWord, " ")
	wordList = RemoveStopWords(wordList, alg.stopWords)

	return wordList
}

func RemoveStopWords(wordList []string, stopWords map[string]string) []string {
	var words []string

	for _, entry := range wordList {
		if _, exists := stopWords[entry]; !exists && entry != " " {
			words = append(words, entry)
		}
	}
	return words
}

func (alg *InvertedIndexAlgorithm) CreateInvertedIndex() *InvertedIndex {
	invertedIndex := &InvertedIndex{
		HashMap: make(map[string]*InvertedIndexEntry),
		Items:   []*InvertedIndexEntry{},
	}
	return invertedIndex
}

func (alg *InvertedIndexAlgorithm) AddItem(Term string, Document int, index *InvertedIndex) {
	if item, exists := index.HashMap[Term]; exists {
		item := &index.Items[item.Index].Document
		*item = append(*item, Document)
	} else {
		InvertedIndexEntry := &InvertedIndexEntry{
			Term:     Term,
			Document: []int{Document},
			Index:    Document,
		}
		index.HashMap[Term] = InvertedIndexEntry
		index.Items = append(index.Items, InvertedIndexEntry)
	}
}
