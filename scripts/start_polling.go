package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/JimbiniBambini/exchanges_data_polling/clients/storj_client"
	"github.com/JimbiniBambini/exchanges_data_polling/common"
	"github.com/JimbiniBambini/exchanges_data_polling/managers/client_manager"
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

type Commander struct {
	Command string `json:"command"`
}
type LoginCredentials struct {
	SateliteKey string `json:"satelite_key"`
	ApiKey      string `json:"api_key"`
	RootPhrase  string `json:"root_phrase"`
}

type LoginCredentialsIf struct {
	Commander
	LoginData LoginCredentials `json:"login_data"`
}

func getClientId(w http.ResponseWriter, r *http.Request, baseUrl string) string {

	// get login data from external request over API
	var reqHandler LoginCredentialsIf
	readBody(&reqHandler, w, r)

	api := "storj_client_manager"

	idGetter := sendReq(baseUrl+api, reqHandler, "POST")
	strResp := extractBetweenQuotes(string(idGetter))
	if strResp == "error" {
		strResp = extractBetweenQuotes(string(sendReq(baseUrl+api, Commander{Command: "list_clients"}, "GET")))
	}

	return strResp
}

type AssetWorker struct {
	ID       string `json:"id"`
	Asset    string `json:"asset"`
	Fiat     string `json:"fiat"`
	Exchange string `json:"exchange"`
	Period   string `json:"period"`
	Run      bool   `json:"run"`
}

type WorkerCommanderIf struct {
	Commander
	ClientId      string      `json:"client_id"`
	BucketKey     string      `json:"bucket_key"`
	PollPeriodSec int         `json:"poll_period"`
	Worker        AssetWorker `json:"worker"`
}

func addWorker(baseUrl string, clientId string, bucketKey string, worker AssetWorker) string {

	var reqHandler WorkerCommanderIf

	reqHandler.Command = "add_worker"
	reqHandler.ClientId = clientId
	reqHandler.BucketKey = bucketKey
	reqHandler.Worker = worker
	respID := extractBetweenQuotes(string(sendReq(baseUrl, reqHandler, "POST")))

	respWorkerId := ""

	if respID == "error" {
		type WorkersList struct {
			Workers []AssetWorker `json:"workers`
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
			self.workersId = append(self.workersId, addWorker(self.baseUrl+api, self.clientId, self.bucketKey, AssetWorker{Asset: asset, Fiat: fiat, Exchange: exchange, Period: period}))
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
			reqHandler.Worker = AssetWorker{
				ID:  workerId,
				Run: true,
			}
			sendReq(self.baseUrl+api, reqHandler, "POST")
		}

	} else {

		for _, workerId := range self.workersId {
			reqHandler.Worker = AssetWorker{
				ID:  workerId,
				Run: false,
			}
			sendReq(self.baseUrl+api, reqHandler, "POST")
		}

	}
}

func Uploader(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
	type CommanderUploader struct {
		Commander
		Worker    workers.AssetWorker `json:"worker"`
		DirFolder string              `json:"dir_folder"`
		Separator string              `json:"file_sep"`
	}
	var endpoint string = "storj_client_ini_upload"

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var commandHandler CommanderUploader
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&commandHandler)

	ctx := context.Background()

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey, "WORKER_ID", commandHandler.Worker.ID)

	if _, ok := clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey]; ok {
		switch r.Method {
		/* ********************* POST ********************* */
		case http.MethodPost:
			if commandHandler.Command == "ini_upload" {
				periodConv, _ := strconv.Atoi(commandHandler.Worker.Period)
				commandHandler.Response = uploadDataFromFolder(ctx, commandHandler.DirFolder,
					&clientManager.Clients[commandHandler.ClientId].StorjClient, commandHandler.BucketKey,
					commandHandler.Worker.Asset, commandHandler.Worker.Fiat, commandHandler.Worker.Exchange, periodConv,
					commandHandler.Separator)
			}
		}
	}

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "RESPONSE:", commandHandler.Response)
	json.NewEncoder(w).Encode(commandHandler.Response)
}

func uploadDataFromFolder(ctx context.Context, dirFolder string, client *storj_client.StorjClient, bucketKey string, asset string, fiat string, exchange string, period int, separatorPath string) bool {
	success := true

	files, err := ioutil.ReadDir(dirFolder)
	if err != nil {
		log.Fatal(err)
		success = false
	}

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
	for idx, fileName := range fileNamesSorted {
		log.Println("Uploading file:", idx+1, "Total Files:", len(fileNamesSorted), "Current Filename:", fileName)

		b, err := ioutil.ReadFile(dirFolder + separatorPath + fileName)
		if err != nil {
			log.Print(err)
			success = false
		}
		success = client.Buckets[bucketKey].UploadObject(ctx, b, common.GenerateBucketObjectKey(asset, fiat, exchange, period, idx), client.Project)
	}
	return success
}

func main() {

	var BUCKET string = "test"
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

	port := os.Getenv("PORT")
	if port == "" {
		port = "8089" // Default port if not specified
	}

	http.ListenAndServe(":"+port, r)
}
