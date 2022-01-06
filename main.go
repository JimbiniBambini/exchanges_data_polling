package main

//module github.com/JimbiniBambini/exchanges_data_polling **latest**

import (
	"bytes"
	"context"
	"data_polling/exchanges_data_polling/common"
	"data_polling/exchanges_data_polling/managers/client_manager"
	"data_polling/exchanges_data_polling/pinger"
	"data_polling/exchanges_data_polling/workers"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

/* ****************************************** ENV AND GLOBALS ****************************************** */
var GIT_DEV bool

// exchange --> asset_regular --> fiat --> exchange_specific_asset
var assetMapping map[string]map[string]map[string]string

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

func manageClients(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
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
			commandHandler.Response = clientManager.LoginHandler(commandHandler.LoginData)
		}

	/* ********************* DELETE ********************* */
	case http.MethodDelete:
		if commandHandler.Command == "delete_client" {

			if _, ok := clientManager.Clients[commandHandler.ClientId]; ok {
				delete(clientManager.Clients, commandHandler.ClientId)
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
			for keyCli, _ := range clientManager.Clients {
				clientIdLst = append(clientIdLst, keyCli)
			}
			commandHandler.Response = clientIdLst
		}

		if commandHandler.Command == "list_bucket_files" {

			filesLst := make([]string, 0)
		loop2:
			for keyCli, client := range clientManager.Clients {
				if keyCli == commandHandler.ClientId {
					for keyBucket, bucket := range client.StorjClient.Buckets {
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
func manageBuckets(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
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
			clientManager.Clients[commandHandler.ClientId].StorjClient.UpdateClient(ctx)
			if clientManager.Clients[commandHandler.ClientId].StorjClient.AddBucket(ctx, commandHandler.BucketKey) {
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
			clientManager.Clients[commandHandler.ClientId].StorjClient.UpdateClient(ctx)
			if clientManager.Clients[commandHandler.ClientId].StorjClient.DeleteBucket(ctx, commandHandler.BucketKey, delOption) {
				commandHandler.Response = "success"
			} else {
				commandHandler.Response = "error"
			}
		}
		/* ********************* GET ********************* */
	case http.MethodGet:
		// get list of buckets for a specific client
		if commandHandler.Command == "list_client_buckets" {
			clientManager.Clients[commandHandler.ClientId].StorjClient.UpdateClient(ctx)
			bucketKeyLst := make([]string, 0)
		loop1:
			for keyCli, client := range clientManager.Clients {
				if keyCli == commandHandler.ClientId {
					for keyBucket, _ := range client.StorjClient.Buckets {
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

func manageFiles(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
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
			clientManager.Clients[commandHandler.ClientId].StorjClient.UpdateClient(ctx)
			clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey].UploadObject(ctx, []byte{1, 2, 3}, commandHandler.FileKey, clientManager.Clients[commandHandler.ClientId].StorjClient.Project)
			//BUCKET.UploadObject(ctx, commandHandler.Data, commandHandler.FileKey, project)

		}
		/* ********************* DELETE ********************* */
	case http.MethodDelete:
		/* ********************* GET ********************* */
	case http.MethodGet:
		if commandHandler.Command == "list_bucket_files" {
			clientManager.Clients[commandHandler.ClientId].StorjClient.UpdateClient(ctx)
			if _, ok := clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey]; ok {

				commandHandler.Response = clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey].GetObjectList()
			} else {
				commandHandler.Response = "error"
			}

		}
		if commandHandler.Command == "download_bucket_file" {
			if data, success := clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey].DownloadObject(ctx, commandHandler.FileKey, clientManager.Clients[commandHandler.ClientId].StorjClient.Project); success {
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
	DataPollPeriodSec int                 `json:"poll_period"`
	Worker            workers.AssetWorker `json:"worker"`
}

func manageWorkers(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
	var endpoint string = "storj_worker_manager"

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var commandHandler CommanderWorker
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&commandHandler)

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey, "WORKER_ID", commandHandler.Worker.ID)

	if _, ok := clientManager.Clients[commandHandler.ClientId]; ok {
		switch r.Method {
		/* ********************* POST ********************* */
		case http.MethodPost:

			// add worker to a specific client
			if commandHandler.Command == "add_worker" {
				mapBytes, _ := json.Marshal(commandHandler.Worker)
				newID := common.CalcRequestBodyCheckSum(mapBytes)

				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey]; !ok {
					clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey] = make(map[string]*workers.AssetWorker)
				}

				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][newID]; !ok {
					commandHandler.Worker.ID = newID
					clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][newID] = &commandHandler.Worker

					commandHandler.Response = newID
				} else {
					commandHandler.Response = "error"
				}
			}

			// start or stop worker
			if commandHandler.Command == "run_worker" {
				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					if commandHandler.Worker.Run && clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].Run == false {
						go func() {
							clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].RunHandler(true)
							clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].Perform(commandHandler.DataPollPeriodSec, commandHandler.BucketKey, clientManager.Clients[commandHandler.ClientId].StorjClient, assetMapping)
							return
						}()
						commandHandler.Response = "success"
					} else {
						commandHandler.Response = "error"
					}
					if !commandHandler.Worker.Run {
						clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].RunHandler(false)
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
				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].RunHandler(false)
					delete(clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey], commandHandler.Worker.ID)
					commandHandler.Response = "success"
				} else {
					commandHandler.Response = "error"
				}
			}
			/* ********************* GET ********************* */
		case http.MethodGet:
			// list all workers assigned to a specific client
			if commandHandler.Command == "list_workers_all" {
				commandHandler.Response = clientManager.Clients[commandHandler.ClientId].Workers
			}
			if commandHandler.Command == "list_workers_bucket" {
				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					commandHandler.Response = clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey]
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

	clientManager := client_manager.NewClientManager()

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
