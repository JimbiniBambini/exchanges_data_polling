package api_manager

import (
	"bytes"
	"context"
	"data_polling/exchanges_data_polling/clients/exchanges"
	"data_polling/exchanges_data_polling/common"
	"data_polling/exchanges_data_polling/workers"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"data_polling/exchanges_data_polling/managers/client_manager"

	"data_polling/exchanges_data_polling/clients/storj_client"
)

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
				commandHandler.Response = "success"
			} else {
				commandHandler.Response = "error"
			}
		}
	/* ********************* DELETE ********************* */
	case http.MethodDelete:
		if commandHandler.Command == "delete_bucket" {
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
				exchanges.ConvertCsvBytes(data, commandHandler.DirDownload)
				commandHandler.Response = string(data)
			} else {
				commandHandler.Response = "error"
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
				if _, ok := clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey]; ok {
					commandHandler.Response = clientManager.Clients[commandHandler.ClientId].Workers[commandHandler.BucketKey]
				} else {
					commandHandler.Response = "error"
				}
			}
		}
	} else {
		commandHandler.Response = "error"
	}

	log.Println("ENDPOINT:", endpoint, "COMMAND:", commandHandler.Command, "RESPONSE:", commandHandler.Response)
	json.NewEncoder(w).Encode(commandHandler.Response)
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
	log.Println(commandHandler)
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
