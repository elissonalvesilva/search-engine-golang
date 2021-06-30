package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/elissonalvesilva/search-light/indexador/algorithms"
	"github.com/elissonalvesilva/search-light/indexador/shared"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var stopWords map[string]string
var wg sync.WaitGroup

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

func Indexador(product map[string]interface{}, index *algorithms.InvertedIndex, alg algorithms.InvertedIndexAlgorithm) {
	if product != nil {
		name := fmt.Sprintf("%v", product["name"])
		tokens := alg.Tokenizer(name)
		for i, token := range tokens {
			alg.AddItem(token, i, index)
		}
	}
	wg.Done()
}

func getFiles(path string) []string {
	var files []string
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if strings.Contains(path, ".gz") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return files
}

func main() {
	stopWordsBytes, stopWordsFile := getDataFromFile("/home/linx/Applications/projects/search-light/indexador/shared/stopwords.txt")
	defer stopWordsFile.Close()
	stopWords = getStopWords(stopWordsBytes)

	invertedIndex := algorithms.NewInvertedIndexAlgorithm(stopWords)

	index := invertedIndex.CreateInvertedIndex()

	files := getFiles("/home/linx/Applications/dumps/puket-vtext/")

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}

		defer f.Close()
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()

		byteValue, _ := ioutil.ReadAll(gr)

		products := strings.Split(string(byteValue), "\n")
		fmt.Println(file, len(products))
		for _, productItem := range products {
			wg.Add(1)
			var product map[string]interface{}
			json.Unmarshal([]byte(productItem), &product)
			go Indexador(product, index, *invertedIndex)
			wg.Wait()
		}

	}
	fmt.Println(len(index.Items))
	defer shared.TimeTrack(time.Now(), "Indexador: ")


	//productsBytes, productFile := getDataFromFile("/home/linx/Applications/dumps/puket-vtext/data")
	//defer productFile.Close()
	//
	//
	//
	//products := strings.Split(string(productsBytes), "\n")
	//fmt.Println(len(products))
	//for _, productItem := range products {
	//	wg.Add(1)
	//	var product map[string]interface{}
	//	json.Unmarshal([]byte(productItem), &product)
	//	go Indexador(product, index, *invertedIndex)
	//	wg.Wait()
	//}
	//fmt.Println(len(index.Items))
}
