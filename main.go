package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type configType struct {
	port           int
	maxConnection  int
	requestTimeout int
	jobCount       int
}

type workerResult struct {
	url    string
	result string
	err    error
}

var (
	connectionsCount    int
	connectionsCountMtx = sync.RWMutex{}
	config              configType
	shutdownIndicator   bool
)

func init() {
	config.port = 8080
	config.maxConnection = 100
	config.requestTimeout = 1
	config.jobCount = 4
}

func main() {
	gracefulStop := make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)
	go func() {
		sig := <-gracefulStop
		log.Printf("Received system call: %+v", sig)
		log.Print("Start shutdown App")
		shutdownIndicator = true
		for {
			if connectionsCount == 0 {
				break
			}
		}
		log.Print("App shutdown")
		os.Exit(0)
	}()

	http.HandleFunc("/post", checkConnectionsCount(postHandler))
	http.ListenAndServe(":"+strconv.Itoa(config.port), nil)
}

func checkConnectionsCount(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if connectionsCount > config.maxConnection || shutdownIndicator {
			log.Print("The maximum number of used connections is exhausted.")
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		} else {
			connectionsCountMtx.Lock()
			connectionsCount++
			connectionsCountMtx.Unlock()
			next.ServeHTTP(w, r)
			connectionsCountMtx.Lock()
			connectionsCount--
			connectionsCountMtx.Unlock()
		}
	}
}

func postHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if len(body) == 0 || r.Header.Get("content-type") != "application/json" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	var urls []string
	err = json.Unmarshal(body, &urls)
	if err != nil || len(urls) > 20 {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	urlsCount := len(urls)
	jobCount := config.jobCount
	if urlsCount < config.jobCount {
		jobCount = urlsCount
	}

	quitChan := make(chan bool, config.jobCount)
	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		println("The client closed the connection prematurely.")
		for i := 0; i < jobCount; i++ {
			quitChan <- true
		}
		return
	}()

	urlsResult, err := getUrlsResult(urls, jobCount, quitChan)
	if err != nil {
		log.Print(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	jsonResult, err := json.Marshal(urlsResult)
	if err != nil {
		log.Print(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(jsonResult))
}

func getUrlsResult(urls []string, jobCount int, quitChan chan bool) (map[string]string, error) {
	var err error
	urlsCount := len(urls)
	result := make(map[string]string)
	inputChan := make(chan string, urlsCount)
	outputChan := make(chan workerResult, urlsCount)

	for i := 0; i < jobCount; i++ {
		go worker(inputChan, outputChan, quitChan)
	}
	for _, url := range urls {
		inputChan <- url
	}
	k := 0
	for item := range outputChan {
		if item.err != nil {
			log.Print(err)
			break
		}
		result[item.url] = item.result
		k++
		if k == urlsCount {
			break
		}
	}
	for i := 0; i < jobCount; i++ {
		quitChan <- true
	}
	return result, err
}

func worker(inputChan <-chan string, outputChan chan<- workerResult, quit <-chan bool) {
	for {
		select {
		case <-quit:
			return
		case url := <-inputChan:
			var data workerResult
			var resp *http.Response
			var body []byte
			data.url = url

			httpClient := &http.Client{
				Timeout: time.Second * time.Duration(config.requestTimeout),
			}
			resp, data.err = httpClient.Get(url)
			if data.err == nil {
				if resp.StatusCode != 200 {
					data.err = errors.New("Error, StatusCode is not 200")
				} else {
					body, data.err = ioutil.ReadAll(resp.Body)
				}
			}
			if data.err == nil {
				data.result = string(body)
			}
			outputChan <- data
		}
	}
}
