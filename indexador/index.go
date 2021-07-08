package indexador

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/elissonalvesilva/search-light/indexador/algorithms"
	"github.com/elissonalvesilva/search-light/indexador/shared"
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
	defer shared.TimeTrack(time.Now(), "Open files: ")
	var files []string
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if strings.Contains(path, ".gz") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		log.Fatal("Err to get files: ")
		panic(err)
	}

	return files
}

func IndexId(id string, index *algorithms.InvertedIndex, alg algorithms.InvertedIndexAlgorithm) {
	fmt.Printf("[ID]: %s \n", id)
	var wg sync.WaitGroup
	wg.Add(1)
	go Indexador(id, index, alg, &wg)
	wg.Wait()
}

func IndexName(name string, index *algorithms.InvertedIndex, alg algorithms.InvertedIndexAlgorithm) {
	fmt.Printf("[NAME]: %s \n", name)
	var wg sync.WaitGroup
	wg.Add(1)
	go Indexador(name, index, alg, &wg)
	wg.Wait()
}

func IndexDetail(details map[string]interface{}, index *algorithms.InvertedIndex, alg algorithms.InvertedIndexAlgorithm) {
	var wg sync.WaitGroup
	for _, detail := range details {
		if reflect.TypeOf(detail).String() == "string" {
			wg.Add(1)
			itemToIndex := fmt.Sprintf("%v", detail)
			if ExistHTMLTag(itemToIndex) {
				wg.Done()
				continue
			}
			fmt.Printf("[DETAIL]: %s \n", itemToIndex)
			go Indexador(itemToIndex, index, alg, &wg)
			wg.Wait()
		}

		if reflect.TypeOf(detail).Kind().String() == "slice" {
			arr := reflect.ValueOf(detail)
			for i := 0; i < arr.Len(); i++ {
				wg.Add(1)
				itemToIndex := fmt.Sprintf("%v", arr.Index(i))
				if ExistHTMLTag(itemToIndex) {
					wg.Done()
					continue
				}
				fmt.Printf("[DETAIL]: %s \n", itemToIndex)
				go Indexador(itemToIndex, index, alg, &wg)
				wg.Wait()
			}
		}
	}
}

func IndexCategories(categories []interface{}, index *algorithms.InvertedIndex, alg algorithms.InvertedIndexAlgorithm) {
	var wg sync.WaitGroup
	for _, cats := range categories {
		var categorie map[string]interface{}

		b, err := json.Marshal(cats)
		if err != nil {
			log.Fatal("Err Index Categorie:")
			panic(err)
		}
		errun := json.Unmarshal(b, &categorie)
		if errun != nil {
			log.Fatal("Err Index Categorie unmarshal:")
			panic(errun)
		}

		for _, cat := range categorie {
			if reflect.TypeOf(cat).String() == "string" {
				wg.Add(1)
				itemToIndex := fmt.Sprintf("%v", cat)
				fmt.Printf("[CATEGORY]: %s \n", itemToIndex)
				go Indexador(itemToIndex, index, alg, &wg)
				wg.Wait()
			}

			if reflect.TypeOf(cat).Kind().String() == "slice" {
				arr := reflect.ValueOf(cat)
				for i := 0; i < arr.Len(); i++ {
					wg.Add(1)
					itemToIndex := fmt.Sprintf("%v", arr.Index(i))
					fmt.Printf("[CATEGORY]: %s \n", itemToIndex)
					go Indexador(itemToIndex, index, alg, &wg)
					wg.Wait()
				}
			}
		}

	}
}

func Index() {
	defer shared.TimeTrack(time.Now(), "Indexador: ")

	stopWordsBytes, stopWordsFile := getDataFromFile("/home/linx/Applications/projects/search-light/indexador/shared/stopwords.txt")
	defer stopWordsFile.Close()

	stopWords = getStopWords(stopWordsBytes)

	invertedIndex := algorithms.NewInvertedIndexAlgorithm(stopWords)

	index := invertedIndex.CreateInvertedIndex()
	var count int64
	files := getFiles("/home/linx/Applications/dumps/bemol-core/")
	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		Process(file, index, invertedIndex, &wg, &count)
		wg.Wait()
	}
	fmt.Println("Indexed Products Total: ", count)
	fmt.Println("Indexed Item Total: ", len(index.Items))
}

func Process(file string, index *algorithms.InvertedIndex, invertedIndex *algorithms.InvertedIndexAlgorithm, wg *sync.WaitGroup, countProducts *int64) {
	rawf, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	lines := make(chan []byte)
	errs := make(chan error)
	done := make(chan bool)

	errProcess := shared.GZLines(rawf, lines, errs, done)
	if errProcess != nil {
		log.Fatal("Err to open file: ", err)
	}

	for {
		select {
		case productItem := <-lines:
			if productItem != nil {
				*countProducts += int64(1)
				var product map[string]interface{}
				json.Unmarshal(productItem, &product)
				if product["id"] != nil {
					id := fmt.Sprintf("%v", product["id"])
					IndexId(id, index, *invertedIndex)
				}

				if product["name"] != nil {
					name := fmt.Sprintf("%v", product["name"])
					IndexName(name, index, *invertedIndex)
				}

				if product["details"] != nil {
					details := product["details"].(map[string]interface{})
					IndexDetail(details, index, *invertedIndex)
				}

				if product["categories"] != nil {
					categories := product["categories"].([]interface{})
					IndexCategories(categories, index, *invertedIndex)
				}
			}
		case err := <-errs:
			if err != nil {
				log.Fatal(err)
			}
		case finish := <-done:
			if finish {
				close(lines)
				close(errs)
				close(done)
				rawf.Close()
				wg.Done()
				return
			}
		}
	}

}

func ExistHTMLTag(str string) bool {
	regexPattern := `<(\"[^\"]*\"|'[^']*'|[^'\">])*>`
	match, _ := regexp.MatchString(regexPattern, str)

	if match {
		return match
	}
	return false
}
func SaveFile(indexs *algorithms.InvertedIndex) {
	out, err := json.Marshal(indexs)
	if err != nil {
		panic(err)
	}
	errSavefile := ioutil.WriteFile("./index", out, 0644)
	check(errSavefile)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
