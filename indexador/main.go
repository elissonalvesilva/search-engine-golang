package main

import (
	"encoding/json"
	"fmt"
	"github.com/elissonalvesilva/search-light/indexador/algorithms"
	"github.com/elissonalvesilva/search-light/indexador/shared"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var stopWords map[string]string

func getDataFromFile(filename string) ([]byte, *os.File) {
	jsonFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	return byteValue, jsonFile
}

func getStopWords(bytes []byte) map[string]string {
	var stopWords = make(map[string]string)
	words := strings.Split(string(bytes), "\n")
	for _, word := range words {
		stopWords[word] = word
	}

	return stopWords
}

func main() {
	productsBytes, productFile := getDataFromFile("/home/linx/Applications/dumps/puket-vtext/data")
	defer productFile.Close()

	stopWordsBytes, stopWordsFile := getDataFromFile("/home/linx/Applications/projects/search-light/indexador/shared/stopwords.txt")
	defer stopWordsFile.Close()
	stopWords = getStopWords(stopWordsBytes)

	products := strings.Split(string(productsBytes), "\n")
	invertedIndex := algorithms.NewInvertedIndexAlgorithm(stopWords)
	index := invertedIndex.CreateInvertedIndex()
	for _, productItem := range products {
		var product map[string]interface{}
		json.Unmarshal([]byte(productItem), &product)
		if product != nil {
			name := fmt.Sprintf("%v", product["name"])
			tokens := invertedIndex.Tokenizer(name)
			for i, token := range tokens {
				invertedIndex.AddItem(token, i, index)
			}
		}
	}

	defer shared.TimeTrack(time.Now(), "Indexador: ")
}
