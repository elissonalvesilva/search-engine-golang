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
	"reflect"
	"strings"
	"sync"
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

func Indexador(itemToIndex string, index *algorithms.InvertedIndex, alg algorithms.InvertedIndexAlgorithm, wg *sync.WaitGroup) {
	tokens := alg.Tokenizer(itemToIndex)
	for i, token := range tokens {
		alg.AddItem(token, i, index)
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

func Index() {
	defer shared.TimeTrack(time.Now(), "Indexador: ")
	stopWordsBytes, stopWordsFile := getDataFromFile("/home/linx/Applications/projects/search-light/indexador/shared/stopwords.txt")
	defer stopWordsFile.Close()
	stopWords = getStopWords(stopWordsBytes)

	invertedIndex := algorithms.NewInvertedIndexAlgorithm(stopWords)

	index := invertedIndex.CreateInvertedIndex()

	enabledIndexDetails := true
	var detailsProducts []map[string]interface{}

	files := getFiles("/home/linx/Applications/dumps/puket-vtext/")
	var wg sync.WaitGroup
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
		for _, productItem := range products {
			if productItem != "" {
				wg.Add(1)
				var product map[string]interface{}
				json.Unmarshal([]byte(productItem), &product)

				itemToIndex := fmt.Sprintf("%v", product["name"])
				go Indexador(itemToIndex, index, *invertedIndex, &wg)
				if enabledIndexDetails {
					details := product["details"].(map[string]interface{})
					detailsProducts = append(detailsProducts, details)
				}
				wg.Wait()
			}
		}

		if enabledIndexDetails {
			for _, v := range detailsProducts {
				for _, detail := range v {
					if reflect.TypeOf(detail).String() == "string" {
						wg.Add(1)
						itemToIndex := fmt.Sprintf("%v", detail)
						go Indexador(itemToIndex, index, *invertedIndex, &wg)
						wg.Wait()
					}

					if reflect.TypeOf(detail).Kind().String() == "slice" {
						arr := reflect.ValueOf(detail)
						for i := 0; i < arr.Len(); i++ {
							wg.Add(1)
							itemToIndex := fmt.Sprintf("%v", arr.Index(i))
							go Indexador(itemToIndex, index, *invertedIndex, &wg)
							wg.Wait()
						}
					}
				}

			}
		}
	}
	fmt.Println(len(index.Items))
}

func main() {
	Index()
	//defer shared.TimeTrack(time.Now(), "Indexador: ")
	//stopWordsBytes, stopWordsFile := getDataFromFile("/home/linx/Applications/projects/search-light/indexador/shared/stopwords.txt")
	//defer stopWordsFile.Close()
	//stopWords = getStopWords(stopWordsBytes)
	//
	//invertedIndex := algorithms.NewInvertedIndexAlgorithm(stopWords)
	//
	//index := invertedIndex.CreateInvertedIndex()
	//
	//productsBytes, productFile := getDataFromFile("/home/linx/Applications/dumps/puket-vtext/data")
	//defer productFile.Close()
	//products := strings.Split(string(productsBytes), "\n")
	//var wg sync.WaitGroup
	//enabledIndexDetails := true
	//var detailsProducts []map[string]interface{}
	//
	//// index name
	//for _, productItem := range products {
	//	if productItem != "" {
	//		wg.Add(1)
	//		var product map[string]interface{}
	//		json.Unmarshal([]byte(productItem), &product)
	//
	//		itemToIndex := fmt.Sprintf("%v", product["name"])
	//		go Indexador(itemToIndex, index, *invertedIndex, &wg)
	//		if enabledIndexDetails {
	//			details := product["details"].(map[string]interface{})
	//			detailsProducts = append(detailsProducts, details)
	//		}
	//		wg.Wait()
	//	}
	//}
	//
	//
	//var waitGroupDetail sync.WaitGroup
	//// index details
	//if enabledIndexDetails {
	//	for _, v := range detailsProducts {
	//		for _, detail := range v {
	//			if reflect.TypeOf(detail).String() == "string" {
	//				waitGroupDetail.Add(1)
	//				itemToIndex := fmt.Sprintf("%v", detail)
	//				go Indexador(itemToIndex, index, *invertedIndex, &waitGroupDetail)
	//				waitGroupDetail.Wait()
	//			}
	//
	//			if reflect.TypeOf(detail).Kind().String() == "slice" {
	//				arr := reflect.ValueOf(detail)
	//				for i := 0; i < arr.Len(); i++ {
	//					waitGroupDetail.Add(1)
	//					itemToIndex := fmt.Sprintf("%v", arr.Index(i))
	//					go Indexador(itemToIndex, index, *invertedIndex, &waitGroupDetail)
	//					waitGroupDetail.Wait()
	//				}
	//			}
	//		}
	//
	//	}
	//}
	//
	//fmt.Println(len(index.Items))
}
