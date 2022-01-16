package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/JimbiniBambini/exchanges_data_polling/common"
	"github.com/JimbiniBambini/exchanges_data_polling/managers/api_manager"
	"github.com/JimbiniBambini/exchanges_data_polling/workers"
	"github.com/gorilla/mux"
)

func sendReq(url string, reqHandler interface{}, reqType string) []byte {
	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(reqHandler)

	req, err := http.NewRequest(reqType, url, bytes.NewBuffer(reqBodyBytes.Bytes()))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return body
}

func extractBetweenQuotes(strIn string) string {
	re := regexp.MustCompile(`"(.*?)"`)
	newStr := re.FindAllString(strIn, -1)[0]

	return newStr[1 : len(newStr)-1]
}

type LoginCredentials struct {
	SateliteKey string `json:"satelite_key"`
	ApiKey      string `json:"api_key"`
	RootPhrase  string `json:"root_phrase"`
}

type LoginCredentialsIf struct {
	api_manager.Commander
	LoginData LoginCredentials `json:"login_data"`
}

func getClientId(w http.ResponseWriter, r *http.Request, baseUrl string) string {

	// get login data from external request over API
	var reqHandler LoginCredentialsIf
	api_manager.ReadBody(&reqHandler, w, r)

	api := "storj_client_manager"

	idGetter := sendReq(baseUrl+api, reqHandler, "POST")
	strResp := extractBetweenQuotes(string(idGetter))
	if strResp == "error" {
		strResp = extractBetweenQuotes(string(sendReq(baseUrl+api, api_manager.Commander{Command: "list_clients"}, "GET")))
	}

	return strResp
}

type WorkerCommanderIf struct {
	api_manager.Commander
	ClientId      string              `json:"client_id"`
	BucketKey     string              `json:"bucket_key"`
	PollPeriodSec int                 `json:"poll_period"`
	Worker        workers.AssetWorker `json:"worker"`
}

func addWorker(baseUrl string, clientId string, bucketKey string, worker workers.AssetWorker) string {

	var reqHandler WorkerCommanderIf

	reqHandler.Command = "add_worker"
	reqHandler.ClientId = clientId
	reqHandler.BucketKey = bucketKey
	reqHandler.Worker = worker
	respID := extractBetweenQuotes(string(sendReq(baseUrl, reqHandler, "POST")))

	respWorkerId := ""

	if respID == "error" {
		type WorkersList struct {
			Workers []workers.AssetWorker `json:"workers`
		}

		var workres WorkersList
		reqHandler.Command = "list_workers_bucket"
		decoder := json.NewDecoder(bytes.NewReader(sendReq(baseUrl, reqHandler, "GET")))
		decoder.Decode(&workres)
		for _, respWorker := range workres.Workers {
			// log.Println(respWorker.ID)
			// log.Println(respWorker.Asset, worker.Asset, respWorker.Fiat, worker.Fiat, respWorker.Exchange, worker.Exchange, respWorker.Period, worker.Period)
			if respWorker.Asset == worker.Asset && respWorker.Fiat == worker.Fiat && respWorker.Exchange == worker.Exchange && respWorker.Period == worker.Period {
				respWorkerId = respWorker.ID
			}
		}
	} else {
		respWorkerId = respID
	}
	return respWorkerId
}

type ClientRunner struct {
	baseUrl   string
	clientId  string
	bucketKey string
	workersId []string
}

func (self *ClientRunner) setup(w http.ResponseWriter, r *http.Request, baseUrl string, bucketKey string) {
	self.baseUrl = baseUrl
	self.bucketKey = bucketKey
	self.clientId = getClientId(w, r, self.baseUrl)
	self.workersId = make([]string, 0)
	self.addWorkers()
}

func (self *ClientRunner) addWorkers() {
	api := "storj_worker_manager"

	fiat := "usd"
	period := "1"

	workersIdMap := make(map[string]map[string]string)
	workersIdMap["btc"] = make(map[string]string)
	workersIdMap["btc"]["kraken"] = "4714115240210224014570463951491583294126247732088732471572367104713122516834"
	workersIdMap["ada"] = make(map[string]string)
	workersIdMap["ada"]["kraken"] = "12318822521130958311118423912322492282411281201902371181659517988678604512018397"
	workersIdMap["ksm"] = make(map[string]string)
	workersIdMap["ksm"]["kraken"] = "14745235246139109223351272051811941092491921012117822872281415422325217991441620219177"
	workersIdMap["dot"] = make(map[string]string)
	workersIdMap["dot"]["kraken"] = "832172382222291502416511129180411711531572552360143981171181361081592256523158232137232"
	workersIdMap["kava"] = make(map[string]string)
	workersIdMap["kava"]["kraken"] = "2051042102291019729841486761997322419114185286521128116541981362101614416054295"

	var reqHandler WorkerCommanderIf
	reqHandler.Command = "run_worker"
	reqHandler.ClientId = self.clientId
	reqHandler.BucketKey = self.bucketKey

	for asset, exchangeMap := range workersIdMap {
		for exchange, _ := range exchangeMap {
			self.workersId = append(self.workersId, addWorker(self.baseUrl+api, self.clientId, self.bucketKey, workers.AssetWorker{Asset: asset, Fiat: fiat, Exchange: exchange, Period: period}))
		}
	}
}

/*
TODO
-> switch to secrets for STORJ login credentials
*/

/* Steps
- login --> get ID
- check workers
	- none available --> add
	- some inactive --> revoke

*/

func (self *ClientRunner) workerRunner(w http.ResponseWriter, r *http.Request, runnerOpti string) {
	api := "storj_worker_manager"
	var reqHandler WorkerCommanderIf
	reqHandler.Command = "run_worker"
	reqHandler.ClientId = self.clientId
	reqHandler.BucketKey = self.bucketKey
	reqHandler.PollPeriodSec = 10

	if runnerOpti == "start" {

		for _, workerId := range self.workersId {
			reqHandler.Worker = workers.AssetWorker{
				ID:  workerId,
				Run: true,
			}
			sendReq(self.baseUrl+api, reqHandler, "POST")
		}

	} else {

		for _, workerId := range self.workersId {
			reqHandler.Worker = workers.AssetWorker{
				ID:  workerId,
				Run: false,
			}
			sendReq(self.baseUrl+api, reqHandler, "POST")
		}

	}
}

func Uploader(w http.ResponseWriter, r *http.Request, baseUrl string, clientId string) {
	type CommanderUploader struct {
		api_manager.Commander
		Worker          workers.AssetWorker `json:"worker"`
		DirFolder       string              `json:"dir_folder"`
		OsFileSeparator string              `json:"file_sep"`
	}

	api := "storj_file_manager"

	var commandHandler CommanderUploader
	api_manager.ReadBody(&commandHandler, w, r)

	success := true

	// get list of available files
	files, err := ioutil.ReadDir(commandHandler.DirFolder)
	log.Println(commandHandler.DirFolder)
	if err != nil {
		log.Println(err)
		success = false
	}

	// get sorted list of available files
	var fileNumbers []int
	var fileNames []string
	var fileNameCut []string

	for _, f := range files {
		if strings.Contains(f.Name(), ".csv") {
			fileNameCut = strings.Split(strings.Split(f.Name(), ".csv")[0], "_")
			num, _ := strconv.Atoi(fileNameCut[len(fileNameCut)-1])
			fileNumbers = append(fileNumbers, num)
			fileNames = append(fileNames, f.Name())

		}
	}
	sort.Ints(fileNumbers)

	// adjust names to match the scheme
	fileNameBase := ""
	for i, val := range fileNameCut {
		if i < len(fileNameCut)-1 {
			fileNameBase += (val + "_")
		}
	}
	var fileNamesSorted []string
	for _, val := range fileNumbers {
		fileNamesSorted = append(fileNamesSorted, fileNameBase+strconv.Itoa(val)+".csv")
	}

	// perform upload
	for idx, fileName := range fileNamesSorted {
		log.Println("Uploading file:", idx+1, "Total Files:", len(fileNamesSorted), "Current Filename:", fileName)

		b, err := ioutil.ReadFile(commandHandler.DirFolder + commandHandler.OsFileSeparator + fileName)
		if err != nil {
			log.Print(err)
			success = false
			break
		}
		if success {
			periodConv, _ := strconv.Atoi(commandHandler.Worker.Period)
			var reqData api_manager.CommanderFiles
			reqData.Command = commandHandler.Command
			reqData.ClientId = clientId //commandHandler.ClientId
			reqData.BucketKey = commandHandler.BucketKey
			reqData.FileKey = common.GenerateBucketObjectKey(commandHandler.Worker.Asset, commandHandler.Worker.Fiat, commandHandler.Worker.Exchange, periodConv, idx)

			reqData.Data = b
			sendReq(baseUrl+api, reqData, "POST")

		}
	}

	json.NewEncoder(w).Encode(commandHandler.Response)
}

func main() {

	var BUCKET string = "test-dev"
	var BASE_URL string = "http://127.0.0.1:8088/"

	var cliRunner ClientRunner
	r := mux.NewRouter()
	r.HandleFunc("/setup", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.setup(w, r, BASE_URL, BUCKET)
		json.NewEncoder(w).Encode(cliRunner.clientId)
	})

	r.HandleFunc("/start_workers", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.workerRunner(w, r, "start")
	})

	r.HandleFunc("/stop_workers", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.workerRunner(w, r, "stop")
	})

	r.HandleFunc("/ini_upload", func(w http.ResponseWriter, r *http.Request) {
		Uploader(w, r, BASE_URL, cliRunner.clientId)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8089" // Default port if not specified
	}

	http.ListenAndServe(":"+port, r)
}
