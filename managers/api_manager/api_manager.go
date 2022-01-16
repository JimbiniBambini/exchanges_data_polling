package api_manager

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/JimbiniBambini/exchanges_data_polling/clients/exchanges"
	"github.com/JimbiniBambini/exchanges_data_polling/common"
	"github.com/JimbiniBambini/exchanges_data_polling/managers/client_manager"
	"github.com/JimbiniBambini/exchanges_data_polling/workers"
)

/* ****************************************** API COMMON ****************************************** */
var operations = map[string]string{
	"success": "success",
	"error":   "error",
}

func ReadBody(structIn interface{}, w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}
	r.Body.Close()
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&structIn)

}

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

func ManageClients(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
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
				commandHandler.Response = operations["success"]
			} else {
				commandHandler.Response = operations["error"]
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

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "RESPONSE:", commandHandler.Response)
	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** BUCKET MANAGER API ****************************************** */
func ManageBuckets(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager, delOption bool) {
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
				commandHandler.Response = operations["success"]
			} else {
				commandHandler.Response = operations["error"]
			}
		}
	/* ********************* DELETE ********************* */
	case http.MethodDelete:
		if commandHandler.Command == "delete_bucket" {
			clientManager.Clients[commandHandler.ClientId].StorjClient.UpdateClient(ctx)
			if clientManager.Clients[commandHandler.ClientId].StorjClient.DeleteBucket(ctx, commandHandler.BucketKey, delOption) {
				commandHandler.Response = operations["success"]
			} else {
				commandHandler.Response = operations["error"]
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

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "RESPONSE:", commandHandler.Response)
	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** FILE MANAGER API ****************************************** */
type CommanderFiles struct {
	Commander
	FileKey     string `json:"file_key"`
	Data        []byte `json:"data"`
	DirDownload string `json:"dir_download"`
	Separator   string `json:"file_sep"`
}

func (self CommanderFiles) saveBytesToCsv() {

}

func ManageFiles(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) {
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
			if clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey].UploadObject(ctx, commandHandler.Data, commandHandler.FileKey, clientManager.Clients[commandHandler.ClientId].StorjClient.Project) {
				commandHandler.Response = operations["success"]
			} else {
				commandHandler.Response = operations["error"]
			}
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
				commandHandler.Response = operations["error"]
			}

		}
		if commandHandler.Command == "download_bucket_file" {
			if data, success := clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey].DownloadObject(ctx, commandHandler.FileKey, clientManager.Clients[commandHandler.ClientId].StorjClient.Project); success {
				exchanges.ConvertCsvBytes(data, commandHandler.DirDownload)
				commandHandler.Response = string(data)
			} else {
				commandHandler.Response = operations["error"]
			}
		}
	}

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "RESPONSE:", commandHandler.Response)
	json.NewEncoder(w).Encode(commandHandler.Response)
}

/* ****************************************** WORKER MANAGER API ****************************************** */
type CommanderWorker struct {
	Commander
	DataPollPeriodSec int                 `json:"poll_period"`
	Worker            workers.AssetWorker `json:"worker"`
}

func ManageWorkers(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager, assetMapping map[string]map[string]map[string]string) {
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
					commandHandler.Response = operations["error"]
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
						commandHandler.Response = operations["success"]
					} else {
						commandHandler.Response = operations["error"]
					}
					if !commandHandler.Worker.Run {
						clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].RunHandler(false)
						commandHandler.Response = operations["success"]
					}

				} else {
					commandHandler.Response = operations["error"]
				}
			}

			/* ********************* DELETE ********************* */
		case http.MethodDelete:
			// delete worker
			if commandHandler.Command == "delete_worker" {
				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID]; ok {
					clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey][commandHandler.Worker.ID].RunHandler(false)
					delete(clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey], commandHandler.Worker.ID)
					commandHandler.Response = operations["success"]
				} else {
					commandHandler.Response = operations["error"]
				}
			}
			/* ********************* GET ********************* */
		case http.MethodGet:
			// list all workers assigned to a specific client
			if commandHandler.Command == "list_workers_all" {
				if len(clientManager.Clients[commandHandler.ClientId].Workers) != 0 {
					commandHandler.Response = clientManager.Clients[commandHandler.ClientId].Workers
				} else {
					commandHandler.Response = make([]workers.AssetWorker, 0)
				}
			}
			if commandHandler.Command == "list_workers_bucket" {
				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey]; ok {
					type WorkersBucket struct {
						Workers []workers.AssetWorker `json:"workers"`
					}
					var workers WorkersBucket
					for _, worker := range clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey] {
						workers.Workers = append(workers.Workers, *worker)
					}

					commandHandler.Response = workers
				} else {
					commandHandler.Response = make([]workers.AssetWorker, 0)
				}
			}
		}
	} else {
		commandHandler.Response = operations["error"]
	}

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "RESPONSE:", commandHandler.Response)
	json.NewEncoder(w).Encode(commandHandler.Response)
}

type CommanderStatus struct {
	Commander
	Filters []string `json:"filters"`
}

type Responder struct {
	ClientId        string   `json:"client_id"`
	Buckets         []string `json:"buckets"`
	Workers         []string `json:"workers"`
	WorkerAssets    []string `json:"worker_assets"`
	LastFilesBucket []string `json:"last_files_in_bucket"` // UPDATE!
}

func StatusManager(w http.ResponseWriter, r *http.Request, clientManager *client_manager.ClientManager) Responder {

	var endpoint string = "storj_client_status_page"

	var responseHandler Responder

	var commandHandler CommanderStatus
	ReadBody(&commandHandler, w, r)

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "CLIENT_ID:", commandHandler.ClientId, "BUCKET_Key", commandHandler.BucketKey)

	if _, ok := clientManager.Clients[commandHandler.ClientId]; ok {
		switch r.Method {
		/* ********************* POST ********************* */
		case http.MethodPost:
			if commandHandler.Command == "get_client_status" {
				responseHandler.ClientId = commandHandler.ClientId
				for _, bucket := range clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets {
					responseHandler.Buckets = append(responseHandler.Buckets, bucket.Key)
				}
			}
			if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey]; ok {
				for _, worker := range clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey] {
					responseHandler.Workers = append(responseHandler.Workers, worker.ID)
					responseHandler.WorkerAssets = append(responseHandler.WorkerAssets, worker.Asset+worker.Fiat)
				}
			}

			if _, ok := clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey]; ok {
				responseHandler.LastFilesBucket = clientManager.Clients[commandHandler.ClientId].StorjClient.Buckets[commandHandler.BucketKey].GetFilteredObjectList(commandHandler.Filters)
			}
		}
	}

	commandHandler.Response = responseHandler
	// json.NewEncoder(w).Encode(commandHandler.Response)

	return responseHandler
}
