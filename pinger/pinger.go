package pinger

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/JimbiniBambini/exchanges_data_polling/config"
	"github.com/JimbiniBambini/exchanges_data_polling/managers/api_manager"
	"github.com/JimbiniBambini/exchanges_data_polling/workers"
)

type RequestFrame struct {
	Msg string `json:"msg"`
}

var OutgoingMsg = RequestFrame{Msg: "Ping"}
var IncomingMsg = RequestFrame{Msg: "Pong"}

func OutgoingMessageHandler(url string) {
	b, err := json.Marshal(OutgoingMsg)
	if err != nil {
		log.Println("OutgoingMessage --> Error while creating json object")
	}

	responseBody := bytes.NewBuffer(b)
	// resp, err := http.Post(url, "application/json", responseBody)
	_, err = http.Post(url, "application/json", responseBody)
	// _, err := http.Post(url, "application/json",)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("OutgoingMessage --> Sent message to:", url)
}

func IncomingMessageHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("IncomingMessage --> Error while reading response body")
	}
	var unpackedMsg RequestFrame
	json.Unmarshal(body, &unpackedMsg)
	log.Println("IncomingMessage --> Received message from:", r.Host, r.URL.Path, "Request:", unpackedMsg.Msg)
	json.NewEncoder(w).Encode(IncomingMsg)
}

func sendReq(url string, reqHandler interface{}, reqType string) []byte {
	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(reqHandler)

	req, err := http.NewRequest(reqType, url, bytes.NewBuffer(reqBodyBytes.Bytes()))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return body
}

func extractBetweenQuotes(strIn string) string {
	re := regexp.MustCompile(`"(.*?)"`)

	newStr := re.FindAllString(strIn, -1)
	if newStr != nil {
		return newStr[0][1 : len(newStr[0])-1]
	} else {
		return "-"
	}
}

func getClientId(baseUrl string) string {

	// get login data from external request over API
	var reqHandler api_manager.CommanderClient
	ravSecretData := strings.Split(os.Getenv("SECRET_DATA"), ",")

	reqHandler.Command = "login_client"
	reqHandler.LoginData = make(map[string]string)
	reqHandler.LoginData["satelite_key"] = ravSecretData[0]
	reqHandler.LoginData["api_key"] = ravSecretData[1]
	reqHandler.LoginData["root_phrase"] = ravSecretData[2]

	api := "storj_client_manager"

	idGetter := sendReq(baseUrl+api, reqHandler, "POST")

	strResp := extractBetweenQuotes(string(idGetter))
	if strResp == "error" {
		strResp = extractBetweenQuotes(string(sendReq(baseUrl+api, api_manager.Commander{Command: "list_clients"}, "GET")))
	}
	return strResp
}

func addWorker(baseUrl string, clientId string, bucketKey string, worker workers.AssetWorker) {

	var reqHandler api_manager.CommanderWorker

	reqHandler.Command = "add_worker"
	reqHandler.ClientId = clientId
	reqHandler.BucketKey = bucketKey
	reqHandler.Worker = worker
	sendReq(baseUrl, reqHandler, "POST")

}

func checkBucketWorkersAndActivate(baseUrl string, clientId string, bucketKey string) {
	var reqHandler api_manager.CommanderWorker
	reqHandler.ClientId = clientId
	if os.Getenv("GIT_DEV") == "true" {
		reqHandler.DataPollPeriodSec = 60
	} else {
		reqHandler.DataPollPeriodSec = 16200
	}
	reqHandler.BucketKey = bucketKey
	type WorkersList struct {
		Workers []workers.AssetWorker `json:"workers"`
	}

	var workres WorkersList
	reqHandler.Command = "list_workers_bucket"
	decoder := json.NewDecoder(bytes.NewReader(sendReq(baseUrl, reqHandler, "GET")))
	decoder.Decode(&workres)

	for _, worker := range workres.Workers {
		reqHandler.Worker = workers.AssetWorker{
			ID:  worker.ID,
			Run: true,
		}
		reqHandler.Command = "run_worker"
		sendReq(baseUrl, reqHandler, "POST")
	}

}

func mainHerokuFkRoutine() {
	baseUrl := "https://data-polling.herokuapp.com/"

	if os.Getenv("GIT_DEV") == "true" {
		baseUrl = "http://127.0.0.1:8088/"
	}

	// storj client login
	clientId := getClientId(baseUrl)

	// configure al assets and exchanges and BUCKETS
	bucketKey := strings.Split(os.Getenv("SECRET_DATA"), ",")[3]
	assetMapping := config.GetAssetConfigMap()

	fiat := "usd"
	period := "1"

	// add all workers
	for asset, exchangeMap := range assetMapping {
		for exchange := range exchangeMap {
			addWorker(baseUrl+"storj_worker_manager", clientId, bucketKey, workers.AssetWorker{Asset: asset, Fiat: fiat, Exchange: exchange, Period: period})
		}
	}

	// check bucket workers status and activate if inactive
	time.Sleep(time.Duration(5) * time.Second)
	checkBucketWorkersAndActivate(baseUrl+"storj_worker_manager", clientId, bucketKey)

}

func PingWorker(urls []string, timerMinCnt int) {

	// Endless loop to ping the server
	for _, url := range urls {
		go func(urlIn string) {
			// Wait for server to start
			time.Sleep(time.Duration(10) * time.Second)
			for {
				//OutgoingMessageHandler(urlIn)
				// mainHerokuFkRoutine()
				time.Sleep(time.Duration(timerMinCnt) * time.Minute)
			}
		}(url)
	}
}
