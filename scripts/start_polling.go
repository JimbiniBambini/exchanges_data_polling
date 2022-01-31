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
	"time"

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
		log.Println(err)
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
			Workers []workers.AssetWorker `json:"workers"`
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
	BaseUrl   string `json:"base_url"`
	clientId  string
	BucketKey string `json:"bucket_key"`
	workersId []string
}

func (self *ClientRunner) setupGeneral(w http.ResponseWriter, r *http.Request) {
	api_manager.ReadBody(self, w, r)
}

func (self *ClientRunner) setupStorj(w http.ResponseWriter, r *http.Request) {
	self.clientId = getClientId(w, r, self.BaseUrl)
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
	workersIdMap["algo"] = make(map[string]string)
	workersIdMap["algo"]["kraken"] = "5312811635216637715941822488816221962121802269177162302211532930230192173412"
	workersIdMap["flow"] = make(map[string]string)
	workersIdMap["flow"]["kraken"] = "19235173134188201222495699512111818617188935210229143108151221157661012723131082"

	var reqHandler WorkerCommanderIf
	reqHandler.Command = "run_worker"
	reqHandler.ClientId = self.clientId
	reqHandler.BucketKey = self.BucketKey

	for asset, exchangeMap := range workersIdMap {
		for exchange, _ := range exchangeMap {
			self.workersId = append(self.workersId, addWorker(self.BaseUrl+api, self.clientId, self.BucketKey, workers.AssetWorker{Asset: asset, Fiat: fiat, Exchange: exchange, Period: period}))
		}
	}
}

/*
TODO
-> set ini_upload to default cliRunner bucket key!
-> add automatic mapping of assets from main project
-> (opti) switch to one click approach
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

	api_manager.ReadBody(&reqHandler, w, r)
	reqHandler.BucketKey = self.BucketKey
	if runnerOpti == "start_workers" {
		// overwrite

		for _, workerId := range self.workersId {

			reqHandler.Worker = workers.AssetWorker{
				ID:  workerId,
				Run: true,
			}
			sendReq(self.BaseUrl+api, reqHandler, "POST")

			log.Println("Switching to the next worker")
			time.Sleep(10 * time.Second)
		}

	} else {

		for _, workerId := range self.workersId {
			reqHandler.Worker = workers.AssetWorker{
				ID:  workerId,
				Run: false,
			}
			sendReq(self.BaseUrl+api, reqHandler, "POST")
		}

	}
}

type CommanderUploader struct {
	api_manager.Commander
	Worker          workers.AssetWorker `json:"worker"`
	DirFolder       string              `json:"dir_folder"`
	OsFileSeparator string              `json:"file_sep"`
}

func Uploader(w http.ResponseWriter, r *http.Request, baseUrl string, clientId string) {

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
			log.Println("Uploading file finished.")
		}
	}

	json.NewEncoder(w).Encode(commandHandler.Response)
}

func Downloader(w http.ResponseWriter, r *http.Request, baseUrl string, clientId string) {
	// bucket key, asset fiat exchange perood
	api := "storj_file_manager"

	var commandHandler CommanderUploader
	api_manager.ReadBody(&commandHandler, w, r)

	// define download handler and rebuild
	var downloadHandler api_manager.CommanderFiles
	downloadHandler.Command = commandHandler.Command
	downloadHandler.ClientId = commandHandler.ClientId
	downloadHandler.BucketKey = commandHandler.BucketKey

	periodInt, _ := strconv.Atoi(commandHandler.Worker.Period)

	// for {
	// 	// get latest asset ID from download folder
	files, err := ioutil.ReadDir(commandHandler.DirFolder)
	if err != nil {
		log.Fatal(err)
	}
	fileLstTmp := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() && strings.Contains(file.Name(), commandHandler.Worker.Asset) && strings.Contains(file.Name(), commandHandler.Worker.Fiat) && strings.Contains(file.Name(), "_period_"+commandHandler.Worker.Period+"_") {
			fileLstTmp = append(fileLstTmp, file.Name())
		}
	}
	_, fileIdLst := common.GetSortedFileNamesAndId(fileLstTmp)
	errorFlag := false

	var lastId int
	if len(fileIdLst) != 0 {
		lastId = fileIdLst[len(fileIdLst)-1]
	} else {
		// -1 because of downloading the next (+1) file id
		lastId = -1
	}
	for {
		if errorFlag {
			break
		} else {

			// 	// get next matching ID from bucket and download on success
			downloadHandler.FileKey = common.GenerateBucketObjectKey(commandHandler.Worker.Asset, commandHandler.Worker.Fiat, commandHandler.Worker.Exchange, periodInt, lastId+1)
			downloadHandler.DirDownload = commandHandler.DirFolder + commandHandler.OsFileSeparator + downloadHandler.FileKey
			json.Unmarshal(sendReq(baseUrl+api, downloadHandler, "GET"), &downloadHandler.Response)
			lastId += 1
			if downloadHandler.Response == "error" {
				break
			}
		}
	}
	// }

	json.NewEncoder(w).Encode(commandHandler.Response)
}

func main() {

	log.Println("Starting application.")

	var cliRunner ClientRunner
	r := mux.NewRouter()
	r.HandleFunc("/setup_general", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.setupGeneral(w, r)
		json.NewEncoder(w).Encode("ok")
	})
	r.HandleFunc("/setup_storj", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.setupStorj(w, r)
		json.NewEncoder(w).Encode(cliRunner.clientId)
	})

	r.HandleFunc("/start_workers", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.workerRunner(w, r, "start_workers")
	})

	r.HandleFunc("/stop_workers", func(w http.ResponseWriter, r *http.Request) {
		cliRunner.workerRunner(w, r, "stop_workers")
	})

	r.HandleFunc("/ini_upload", func(w http.ResponseWriter, r *http.Request) {
		Uploader(w, r, cliRunner.BaseUrl, cliRunner.clientId)
	})

	r.HandleFunc("/download_asset", func(w http.ResponseWriter, r *http.Request) {
		Downloader(w, r, cliRunner.BaseUrl, cliRunner.clientId)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8089" // Default port if not specified
	}

	http.ListenAndServe(":"+port, r)
}
