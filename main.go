package main

import (
	"bytes"
	"context"
	"data_polling/clients/exchanges"
	"data_polling/clients/storj_client"
	"data_polling/common"
	"data_polling/config"
	"data_polling/pinger"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

/* ****************************************** ENV AND GLOBALS ****************************************** */
var GIT_DEV bool

// exchange --> asset_regular --> fiat --> exchange_specific_asset
var assetMapping map[string]map[string]map[string]string

/* ****************************************** CLIENT IMPLEMENTATION ****************************************** */
type Client struct {
	ID          string
	storjClient storj_client.StorjClient
	workers     map[string]map[string]*AssetWorker //scheme: bucket --> worker_id
}

type ClientManager struct {
	clients map[string]*Client
}

func NewClientManager() ClientManager {
	var clientManager ClientManager
	clientManager.clients = make(map[string]*Client, 0)
	return clientManager
}

func (clientManager *ClientManager) loginHandler(loginCredentials map[string]string) string {
	success := "error"
	mapBytes, _ := json.Marshal(loginCredentials)
	newID := common.CalcRequestBodyCheckSum(mapBytes)
	alreadyAvailable := false
	for _, cli := range clientManager.clients {
		if cli.ID == newID {
			alreadyAvailable = true
			break
		}
	}
	if !alreadyAvailable {
		ctx := context.Background()

		newClient, cliAvailable := storj_client.NewStorjClient(ctx, loginCredentials)
		if cliAvailable {
			clientManager.clients[newID] = &Client{
				ID:          newID,
				storjClient: newClient,
				workers:     make(map[string]map[string]*AssetWorker, 0),
			}
			success = newID
		}
	}

	return success
}

/* ****************************************** WORKER IMPLEMENTATION ****************************************** */
type AssetWorker struct {
	ID       string `json:"id"`
	Asset    string `json:"asset"`
	Fiat     string `json:"fiat"`
	Exchange string `json:"exchange"`
	Period   string `json:"period"`
	Run      bool   `json:"run"`
}

func (self *AssetWorker) runHandler(runCmd bool) {
	self.Run = runCmd
}

func (self *AssetWorker) perform(waitTimeSec int, bucketKey string, storjClient storj_client.StorjClient) {
	// 	// get latest data for exchange and asset from storj
	ctx := context.Background()

	// 	// main loop
	var cntSec int = 0
	for {
		if self.Run {
			if cntSec == 0 {
				//storjClient.GetAllBucketsAndObjects(ctx)
				storjClient.UpdateClient(ctx)
				fileLstAsset := make([]string, 0)
				for _, obj := range storjClient.Buckets[bucketKey].GetObjectList() {
					//fmt.Println("HERE1:", obj)
					if strings.Contains(obj, self.Asset) && strings.Contains(obj, self.Fiat) && strings.Contains(obj, self.Exchange) {
						fileLstAsset = append(fileLstAsset, obj)
					}
				}
				_, idx := common.GetSortedFileNamesAndId(fileLstAsset)

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

				storjClient.Buckets[bucketKey].UploadObject(ctx, bytes2Upload, common.GenerateBucketObjectKey(self.Asset, self.Fiat, self.Exchange, period, newIdx), storjClient.Project)

				// --> repeat after WAIT_TIME
				log.Println("worker_id:", self.ID, "next_call_in:", waitTimeSec, "seconds")

			}
			cntSec += 1
			time.Sleep(1 * time.Second)
			if cntSec == waitTimeSec {
				cntSec = 0
			}
		} else {
			return
		}
	}
}

/* ****************************************** API COMMON ****************************************** */

type Commander struct {
	Command   string      `json:"command"`
	ClientId  string      `json:"client_id"`
	BucketKey string      `json:"bucket_key"`
	Response  interface{} `json:"response"`
}

/* ****************************************** CLIENT MANAGER API ****************************************** */
type CommanderClient struct {
	Commander
	LoginData map[string]string `json:"login_data"`
}

func manageClients(w http.ResponseWriter, r *http.Request, clientManager *ClientManager) {
	var endpoint string = "storj_client_manager"

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var commandHandler CommanderClient
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&commandHandler)

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey)

	switch r.Method {
	/* ********************* POST ********************* */
	case http.MethodPost:
		if commandHandler.Command == "login_client" {
			commandHandler.Response = clientManager.loginHandler(commandHandler.LoginData)
		}

	/* ********************* DELETE ********************* */
	case http.MethodDelete:
		if commandHandler.Command == "delete_client" {

			if _, ok := clientManager.clients[commandHandler.ClientId]; ok {
				delete(clientManager.clients, commandHandler.ClientId)
				commandHandler.Response = "success"
			} else {
				commandHandler.Response = "error"
			}
		}

		/* ********************* GET ********************* */
	case http.MethodGet:

		// get list of clients
		if commandHandler.Command == "list_clients" {

			clientIdLst := make([]string, 0)
			for keyCli, _ := range clientManager.clients {
				clientIdLst = append(clientIdLst, keyCli)
			}
			commandHandler.Response = clientIdLst
		}

		if commandHandler.Command == "list_bucket_files" {

			filesLst := make([]string, 0)
		loop2:
			for keyCli, client := range clientManager.clients {
				if keyCli == commandHandler.ClientId {
					for keyBucket, bucket := range client.storjClient.Buckets {
						if keyBucket == commandHandler.BucketKey {
							for _, obj := range bucket.Objects {
								//log.Println(obj, bucket.Objects)
								filesLst = append(filesLst, obj.Key)
							}
							break loop2
						}
					}
				}
			}
			filesSortedLst := make([]string, 0)

			if len(filesLst) != 0 {
				filesSortedLst = common.GetSortedFilelistExchange(filesLst)
			}
			commandHandler.Response = filesSortedLst
		}

	}

	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** BUCKET MANAGER API ****************************************** */
func manageBuckets(w http.ResponseWriter, r *http.Request, clientManager *ClientManager) {
	var endpoint string = "storj_bucket_manager"

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var commandHandler Commander
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&commandHandler)

	ctx := context.Background()

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey)

	switch r.Method {
	/* ********************* POST ********************* */
	case http.MethodPost:
		if commandHandler.Command == "add_bucket" {
			clientManager.clients[commandHandler.ClientId].storjClient.UpdateClient(ctx)
			if clientManager.clients[commandHandler.ClientId].storjClient.AddBucket(ctx, commandHandler.BucketKey) {
				commandHandler.Response = "success"
			} else {
				commandHandler.Response = "error"
			}
		}
	/* ********************* DELETE ********************* */
	case http.MethodDelete:
		if commandHandler.Command == "delete_bucket" {
			var delOption bool = false
			if GIT_DEV {
				delOption = true
			}
			clientManager.clients[commandHandler.ClientId].storjClient.UpdateClient(ctx)
			if clientManager.clients[commandHandler.ClientId].storjClient.DeleteBucket(ctx, commandHandler.BucketKey, delOption) {
				commandHandler.Response = "success"
			} else {
				commandHandler.Response = "error"
			}
		}
		/* ********************* GET ********************* */
	case http.MethodGet:
		// get list of buckets for a specific client
		if commandHandler.Command == "list_client_buckets" {
			clientManager.clients[commandHandler.ClientId].storjClient.UpdateClient(ctx)
			bucketKeyLst := make([]string, 0)
		loop1:
			for keyCli, client := range clientManager.clients {
				if keyCli == commandHandler.ClientId {
					for keyBucket, _ := range client.storjClient.Buckets {
						bucketKeyLst = append(bucketKeyLst, keyBucket)
					}
					break loop1
				}

			}
			commandHandler.Response = bucketKeyLst
		}
	}
	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** FILE MANAGER API ****************************************** */
type CommanderFiles struct {
	Commander
	FileKey string `json:"file_key"`
	Data    []byte `json:"data"`
}

func manageFiles(w http.ResponseWriter, r *http.Request, clientManager *ClientManager) {
	var endpoint string = "storj_file_manager"

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var commandHandler CommanderFiles
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&commandHandler)

	ctx := context.Background()

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey)

	switch r.Method {
	/* ********************* POST ********************* */
	case http.MethodPost:
		if commandHandler.Command == "add_bucket_file" {
			clientManager.clients[commandHandler.ClientId].storjClient.UpdateClient(ctx)
			clientManager.clients[commandHandler.ClientId].storjClient.Buckets[commandHandler.BucketKey].UploadObject(ctx, []byte{1, 2, 3}, commandHandler.FileKey, clientManager.clients[commandHandler.ClientId].storjClient.Project)
			//BUCKET.UploadObject(ctx, commandHandler.Data, commandHandler.FileKey, project)

		}
		/* ********************* DELETE ********************* */
	case http.MethodDelete:
		/* ********************* GET ********************* */
	case http.MethodGet:
		if commandHandler.Command == "list_bucket_files" {
			clientManager.clients[commandHandler.ClientId].storjClient.UpdateClient(ctx)
			if _, ok := clientManager.clients[commandHandler.ClientId].storjClient.Buckets[commandHandler.BucketKey]; ok {

				commandHandler.Response = clientManager.clients[commandHandler.ClientId].storjClient.Buckets[commandHandler.BucketKey].GetObjectList()
			} else {
				commandHandler.Response = "error"
			}

		}
		if commandHandler.Command == "download_bucket_file" {
			if data, success := clientManager.clients[commandHandler.ClientId].storjClient.Buckets[commandHandler.BucketKey].DownloadObject(ctx, commandHandler.FileKey, clientManager.clients[commandHandler.ClientId].storjClient.Project); success {
				type RespFrame struct {
					Data []byte `json:"data"`
				}
				commandHandler.Response = RespFrame{Data: data}
			} else {
				commandHandler.Response = "error"
			}
		}
	}
	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** WORKER MANAGER API ****************************************** */
type CommanderWorker struct {
	Commander
	DataPollPeriodSec int         `json:"poll_period"`
	Worker            AssetWorker `json:"worker"`
}

func manageWorkers(w http.ResponseWriter, r *http.Request, clientManager *ClientManager) {
	var endpoint string = "storj_worker_manager"

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var commandHandler CommanderWorker
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&commandHandler)

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey, "WORKER_ID", commandHandler.Worker.ID)

	if _, ok := clientManager.clients[commandHandler.ClientId]; ok {
		switch r.Method {
		/* ********************* POST ********************* */
		case http.MethodPost:

			// add worker to a specific client
			if commandHandler.Command == "add_worker" {
				mapBytes, _ := json.Marshal(commandHandler.Worker)
				newID := common.CalcRequestBodyCheckSum(mapBytes)

				if _, ok := clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey]; !ok {
					clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey] = make(map[string]*AssetWorker)
				}

				if _, ok := clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][newID]; !ok {
					commandHandler.Worker.ID = newID
					clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][newID] = &commandHandler.Worker

					commandHandler.Response = newID
				} else {
					commandHandler.Response = "error"
				}
			}

			// start or stop worker
			if commandHandler.Command == "run_worker" {
				if _, ok := clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					if commandHandler.Worker.Run && clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID].Run == false {
						go func() {
							clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID].runHandler(true)
							clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID].perform(commandHandler.DataPollPeriodSec, commandHandler.BucketKey, clientManager.clients[commandHandler.ClientId].storjClient)
							return
						}()
						commandHandler.Response = "success"
					} else {
						commandHandler.Response = "error"
					}
					if !commandHandler.Worker.Run {
						clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID].runHandler(false)
						commandHandler.Response = "success"
					}

				} else {
					commandHandler.Response = "error"
				}
			}

			/* ********************* DELETE ********************* */
		case http.MethodDelete:
			// delete worker
			if commandHandler.Command == "delete_worker" {
				if _, ok := clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID].runHandler(false)
					delete(clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey], commandHandler.Worker.ID)
					commandHandler.Response = "success"
				} else {
					commandHandler.Response = "error"
				}
			}
			/* ********************* GET ********************* */
		case http.MethodGet:
			// list all workers assigned to a specific client
			if commandHandler.Command == "list_workers_all" {
				commandHandler.Response = clientManager.clients[commandHandler.ClientId].workers
			}
			if commandHandler.Command == "list_workers_bucket" {
				if _, ok := clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					commandHandler.Response = clientManager.clients[commandHandler.ClientId].workers[commandHandler.BucketKey]
				} else {
					commandHandler.Response = "error"
				}
			}
		}
	} else {
		commandHandler.Response = "error"
	}
	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** TODO ****************************************** */
/*

	NEXT Release
	-+ add variables for dev and prod versions
	+ add option for multiple clients
	+ add option to delete clients
	+ add option to delete workers
	+ start/stop workers
	+ check with multiple workers
	+ list all available buckets in client
	+ list all available files in bucket
	- api to upload files

	NEXT Release
	- correct the issue with panic, while downloading non existing file
	- add download scheme and proper api for exchanges (at least kraken)
	- check the option for separate maps for clients and workers (workers can be accessed via client id as a map key) --> better scalability?
	- switch to new architecture with separate module for APi to clean the main module
	- add option for clients with multiple buckets at the same time
	- ADD PROPER CONFIG FOR ASSETS
	- ADD CONCAT-FILE ROUTINE AND A BUCKET FOR IT
*/

/* ****************************************** MAIN ****************************************** */
func main() {

	log.Println("Dev_Env:", os.Getenv("GIT_DEV") == "true")
	GIT_DEV = (os.Getenv("GIT_DEV") == "true")

	clientManager := NewClientManager()

	assetMapping = make(map[string]map[string]map[string]string)
	assetMapping["kraken"] = make(map[string]map[string]string)
	assetMapping["kraken"]["btc"] = make(map[string]string)
	assetMapping["kraken"]["btc"]["usd"] = "XXBTZUSD"
	assetMapping["kraken"]["ada"] = make(map[string]string)
	assetMapping["kraken"]["ada"]["usd"] = "ADAUSD"

	r := mux.NewRouter()

	r.HandleFunc("/storj_client_manager", func(w http.ResponseWriter, r *http.Request) {
		manageClients(w, r, &clientManager)
	})

	r.HandleFunc("/storj_bucket_manager", func(w http.ResponseWriter, r *http.Request) {
		manageBuckets(w, r, &clientManager)
	})

	r.HandleFunc("/storj_file_manager", func(w http.ResponseWriter, r *http.Request) {
		manageFiles(w, r, &clientManager)
	})

	r.HandleFunc("/storj_worker_manager", func(w http.ResponseWriter, r *http.Request) {
		manageWorkers(w, r, &clientManager)
	})

	r.HandleFunc("/ping_in", func(w http.ResponseWriter, r *http.Request) {
		log.Println("incoming message", w)
		pinger.IncomingMessageHandler(w, r)
	})

	if GIT_DEV {
		pinger.PingWorker([]string{"http://127.0.0.1:8088/ping_in"}, 1)
	} else {
		pinger.PingWorker([]string{"https://data-polling.herokuapp.com/ping_in"}, 1)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8088" // Default port if not specified
	}

	http.ListenAndServe(":"+port, r)
}
