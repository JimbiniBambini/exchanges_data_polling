package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"data_polling/clients/exchanges"
	"data_polling/clients/storj_client"
	"data_polling/config"
	//"data_polling/pinger"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

//"data_polling/clients/storj_client"
// "data_polling/config"
// "data_polling/clients/exchanges"

//"github.com/gorilla/mux"

// exchange --> asset_regular --> fiat --> exchange_specific_asset
var assetMapping map[string]map[string]map[string]string

type RespFrame struct {
	NewID string `json:"new_id"`
}

func calcRequestBodyCheckSum(body []byte) string {
	h := sha256.New()
	r := bytes.NewReader(body)
	if _, err := io.Copy(h, r); err != nil {
		log.Fatal(err)
		return ""
	}
	var str2ret string
	for _, val := range h.Sum(nil) {
		str2ret += strconv.Itoa(int(val))
	}

	return str2ret
}

func generateBucketObjectKey(asset string, assetFiat string, exchange string, period int, objectIdx int) string {
	keyFinal := fmt.Sprintf("%s_%s_%s_period_%d_id_%d.csv", asset, assetFiat, exchange, period, objectIdx)

	return keyFinal
}

func makeInitialSingleFileUpload(ctx context.Context, self *storj_client.StorjClient, asset string, fiat string, exchange string, period int, path2Folder string) {
	// ensure bucket --> Version + Pair + Option + Exchange
	var keys []string = []string{self.Version, asset, exchange, storj_client.StorageOptionSingle}

	_, err := self.Project.EnsureBucket(ctx, keys[0])
	if err != nil {
		fmt.Errorf("could not ensure bucket: %v", err)
	}
	self.GetAllBucketsAndObjects(ctx)
	files, err := ioutil.ReadDir(path2Folder)
	if err != nil {
		log.Fatal(err)
	}

	var fileNumbers []int
	var fileNames []string
	var fileNameCut []string

	for _, f := range files {
		if strings.Contains(f.Name(), ".csv") {
			//fmt.Println(f.Name(), strings.Contains(f.Name(), ".csv"))
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

	for _, val := range self.Buckets {
		if val.Key == self.Version {
			for idx, fileName := range fileNamesSorted {
				fmt.Println("Uploading file:", idx+1, "Total Files:", len(fileNamesSorted))
				b, err := ioutil.ReadFile(path2Folder + string(filepath.Separator) + fileName)
				if err != nil {
					fmt.Print(err)
				}
				fmt.Println(generateBucketObjectKey(asset, fiat, exchange, period, idx))
				val.UploadObject(ctx, b, generateBucketObjectKey(asset, fiat, exchange, period, idx), self.Project)
				//fmt.Println(b)
			}
		}
	}

}

func getSortedFileNamesAndId(files []string) ([]string, []int) {
	var fileNumbers []int
	var fileNames []string
	var fileNameCut []string
	for _, f := range files {
		if strings.Contains(f, ".csv") {
			//fmt.Println(f.Name(), strings.Contains(f.Name(), ".csv"))
			fileNameCut = strings.Split(strings.Split(f, ".csv")[0], "_")
			num, _ := strconv.Atoi(fileNameCut[len(fileNameCut)-1])
			fileNumbers = append(fileNumbers, num)
			fileNames = append(fileNames, f)
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

	return fileNamesSorted, fileNumbers
}

func getFileNameStructure(fileNameIn string) (string, string) {
	// Return vars
	var fileExtension string
	var baseStructure string

	// Impl
	stringConstructor := strings.Split(fileNameIn, ".")
	fileExtension = "." + stringConstructor[len(stringConstructor)-1]
	var baseNameConstructor string = strings.Split(fileNameIn, fileExtension)[0]
	fileNameStructureSplit := strings.Split(baseNameConstructor, "_")
	for _, val := range fileNameStructureSplit[:len(fileNameStructureSplit)-1] {
		baseStructure += (val + "_")
	}

	return baseStructure, fileExtension
}

func server() {

	// API (Storj Credentials)
	// get Storj credentials (aka config per POST)
	// login by Storj
	// by success send master ID
	// develop system to stop all workers
	// develop erase mechanism for IDs
	// API (OPTIONAL Initial Upload)
	// option to make initial upload
	// API (Add worker to Storj ID)
	// check storj internal ID
}

func generateRandomID(size int) string {
	rand.Seed(time.Now().UnixNano())
	min := 0
	max := 9
	var randStr string
	for i := 0; i < size; i++ {
		randStr += strconv.Itoa(rand.Intn(max-min+1) + min)
	}
	return randStr
}

type AssetWorker struct {
	ID       string `json:"id"`
	Asset    string `json:"asset"`
	Fiat     string `json:"fiat"`
	Exchange string `json:"exchange"`
	Period   string `json:"period"`
	Run      bool   `json:"run"`
}

func (self AssetWorker) perform(waitTimeSec int, storjClient storj_client.StorjClient) {
	// 	// get latest data for exchange and asset from storj
	ctx := context.Background()

	// 	// main loop
	var cntSec int = 0
	for {
		if self.Run {
			if cntSec == 0 {
				storjClient.GetAllBucketsAndObjects(ctx)
				fileLstAsset := make([]string, 0)
				for _, obj := range storjClient.Buckets[storjClient.Version].GetObjectList() {
					//fmt.Println("HERE1:", obj)
					if strings.Contains(obj, self.Asset) && strings.Contains(obj, self.Fiat) && strings.Contains(obj, self.Exchange) {
						fileLstAsset = append(fileLstAsset, obj)
					}
				}
				_, idx := getSortedFileNamesAndId(fileLstAsset)

				// get data from exchange
				var bytes2Upload []byte = nil

				configExchange := config.NewExchangeConfig(self.Exchange)
				// fmt.Println("Worker", self)
				// fmt.Println("Exchange Config", configExchange)
				for _, assetExchange := range configExchange.Assets {
					if assetExchange == assetMapping[self.Exchange][self.Asset][self.Fiat] {
						// fmt.Println("HERE AFTER CONFIG", assetMapping)
						bytes2Upload = exchanges.GetExchangeDataCsvByte(assetExchange, *configExchange)
					}
				}

				// upload
				newIdx := 0
				if len(idx) > 0 {
					newIdx = idx[len(idx)-1] + 1
				}
				period, _ := strconv.Atoi(configExchange.DataPeriod) // BINANCE!!!!!! --> is 1m for minute. Others may also differ!!!

				storjClient.Buckets[storjClient.Version].UploadObject(ctx, bytes2Upload, generateBucketObjectKey(self.Asset, self.Fiat, self.Exchange, period, newIdx), storjClient.Project)

				// --> repeat after WAIT_TIME
				log.Println("worker_id:", self.ID, "next_call_in:", waitTimeSec, "seconds")

			}
			cntSec += 1
			time.Sleep(1 * time.Second)
			if cntSec == waitTimeSec {
				cntSec = 0
			}
		}
	}
}

type Client struct {
	ID          string
	storjClient storj_client.StorjClient
	workers     map[string]AssetWorker
}

type ClientManager struct {
	clients map[string]Client
}

func NewClientManager() ClientManager {
	var clientManager ClientManager
	clientManager.clients = make(map[string]Client, 0)
	return clientManager
}

func (clientManager *ClientManager) loginManager(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("Error while reading request body")
		}

		newID := calcRequestBodyCheckSum(body) //generateRandomID(storj_client.LenID)

		alreadyAvailable := false
		for _, cli := range clientManager.clients {
			if cli.ID == newID {
				alreadyAvailable = true
				break
			}
		}

		if !alreadyAvailable {
			ctx := context.Background()

			confLogin := storj_client.NewAccessCredentialsStorj(body)
			newClient := confLogin.GetStorjClient(ctx)

			if newClient.Project != nil && newClient.Buckets != nil && newClient.Version != "error" {
				clientManager.clients[newID] = Client{
					ID:          newID,
					storjClient: newClient,
					workers:     make(map[string]AssetWorker, 0),
				}
				log.Println("new_client_id:", newID)
				json.NewEncoder(w).Encode(RespFrame{NewID: newID})
			} else {
				json.NewEncoder(w).Encode(RespFrame{NewID: "wrong_credentials"})
			}
		} else {
			json.NewEncoder(w).Encode(RespFrame{NewID: "error"})
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (clientManager *ClientManager) workerManager(w http.ResponseWriter, r *http.Request) {
	//clientID string, asset string, fiat string, exchange string, period string
	switch r.Method {
	case http.MethodPost:
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Println("Error while reading request body")
		}

		bodyHandler := make(map[string]string)
		decoder := json.NewDecoder(bytes.NewReader(body))
		err = decoder.Decode(&bodyHandler)

		newID := calcRequestBodyCheckSum(body) //generateRandomID(storj_client.LenID)
		// check client
		if _, ok := clientManager.clients[bodyHandler["client_id"]]; ok {
			alreadyAvailable := false
			for _, worker := range clientManager.clients[bodyHandler["client_id"]].workers {
				if worker.ID == newID {
					alreadyAvailable = true
					break
				}
			}

			if !alreadyAvailable {
				var worker AssetWorker
				decoder := json.NewDecoder(bytes.NewReader(body))
				err = decoder.Decode(&worker)
				worker.ID = newID
				clientManager.clients[bodyHandler["client_id"]].workers[newID] = worker
				log.Println("new_worker_id:", newID)
				if worker.Run {
					log.Println("worker_start:", newID, worker.Asset, worker.Fiat, worker.Exchange, worker.Period)
					go func() {
						// clientManager.clients[bodyHandler["client_id"]].workers[newID].perform(60, clientManager.clients[bodyHandler["client_id"]].storjClient)
						clientManager.clients[bodyHandler["client_id"]].workers[newID].perform(16200, clientManager.clients[bodyHandler["client_id"]].storjClient)
					}()
				}
			}
		} else {
			json.NewEncoder(w).Encode(RespFrame{NewID: "no_client_found"})
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func main() {
	clientManager := NewClientManager()

	assetMapping = make(map[string]map[string]map[string]string)
	assetMapping["kraken"] = make(map[string]map[string]string)
	assetMapping["kraken"]["btc"] = make(map[string]string)
	assetMapping["kraken"]["btc"]["usd"] = "XXBTZUSD"
	assetMapping["kraken"]["ada"] = make(map[string]string)
	assetMapping["kraken"]["ada"]["usd"] = "ADAUSD"

	r := mux.NewRouter()

	r.HandleFunc("/storj_client_login", func(w http.ResponseWriter, r *http.Request) {
		clientManager.loginManager(w, r)
	})

	r.HandleFunc("/storj_client_add_worker", func(w http.ResponseWriter, r *http.Request) {
		clientManager.workerManager(w, r)
	})

	r.HandleFunc("/storj_client_ini_upload", func(w http.ResponseWriter, r *http.Request) {
		clientManager.loginManager(w, r)
		fmt.Println(clientManager)
		ctx := context.Background()
		// version := "173183936941892022121511823362214322081317511017165981032221661042297013161216231130"		//p1.0
		version := "20412222322359501204117822018212110510788953972494456755024359103164184190215115113" // d1.0
		clientTmp := clientManager.clients[version].storjClient
		makeInitialSingleFileUpload(ctx, &clientTmp, "btc", "usd", "kraken", 1, "/Users/Slava/SW_Projects/git/PrivateProjects/trading_pattern_bot/csv_data/exchange_api_data/kraken/1/xbtusd")
	})

	r.HandleFunc("/ping_in", func(w http.ResponseWriter, r *http.Request) {
		log.Println("incoming message", w)
		//pinger.IncomingMessageHandler(w, r)
	})

	//pinger.PingWorker([]string{"http://127.0.0.1:8088/ping_in"}, 1)
	//pinger.PingWorker([]string{"https://data-polling.herokuapp.com/ping_in"}, 1)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8088" // Default port if not specified
	}

	http.ListenAndServe(":"+port, r)

	// ADD PROPER CONFIG FOR ASSETS
	// ADD CONCAT-FILE ROUTINE AND A BUCKET FOR IT

	// var storjClient storj_client.StorjClient
	// ctx := context.Background()
	// storjClient.AccessProject(ctx, config.NewAccessCredentialsStorj())
	// storjClient.GetAllBucketsAndObjects(ctx)

	// // NOOOOOOOO!!!!!!!!!
	//makeInitialSingleFileUpload(ctx, &storjClient, "btc", "usd", "kraken", 1, "/Users/Slava/SW_Projects/git/PrivateProjects/trading_pattern_bot/csv_data/exchange_api_data/kraken/1/xbtusd")
	// // NOOOOOOOO!!!!!!!!!

	// assetWorker("btc", "usd", "kraken")

	//bucketNr := 0
	//dataDownload, _ := storjClient.Buckets[bucketNr].DownloadObject(ctx, "btc_usd_kraken_period_1_id_0.csv", storjClient.Project)
	//	fmt.Println(len(storjClient.Buckets[bucketNr].Objects), strings.Split(strings.Split(string(dataDownload), "\n")[0], ","))
	//fmt.Println(dataDownload)

	// configKraken := config.NewExchangeConfig("kraken")
	// for id, asset := range configKraken.Assets {
	// 	if id == 0 {
	// 		fmt.Println(asset)
	// 		//exchanges.GetExchangeData(asset, *configKraken)
	// 		fmt.Println(exchanges.GetExchangeDataCsvByte(asset, *configKraken))
	// 	}

	// }

}
