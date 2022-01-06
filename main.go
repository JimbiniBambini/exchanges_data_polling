package main

//module github.com/JimbiniBambini/exchanges_data_polling **latest**

import (
	"data_polling/exchanges_data_polling/managers/api_manager"
	"data_polling/exchanges_data_polling/managers/client_manager"
	"data_polling/exchanges_data_polling/pinger"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

/* ****************************************** ENV AND GLOBALS ****************************************** */
var GIT_DEV bool

// exchange --> asset_regular --> fiat --> exchange_specific_asset
var assetMapping map[string]map[string]map[string]string

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
		api_manager.ManageClients(w, r, &clientManager)
	})

	r.HandleFunc("/storj_bucket_manager", func(w http.ResponseWriter, r *http.Request) {
		api_manager.ManageBuckets(w, r, &clientManager, GIT_DEV)
	})

	r.HandleFunc("/storj_file_manager", func(w http.ResponseWriter, r *http.Request) {
		api_manager.ManageFiles(w, r, &clientManager)
	})

	r.HandleFunc("/storj_worker_manager", func(w http.ResponseWriter, r *http.Request) {
		api_manager.ManageWorkers(w, r, &clientManager, assetMapping)
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
