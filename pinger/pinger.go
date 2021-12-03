package pinger

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
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
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		log.Fatalln(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("OutgoingMessage --> Error while reading request body")
	}

	var unpackedMsg RequestFrame
	json.Unmarshal(body, &unpackedMsg)

	log.Println("OutgoingMessage --> Sent message to:", url, "Response:", unpackedMsg.Msg)
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

func PingWorker(urls []string, timerMinCnt int) {
	// create endless routine for every url
	for _, url := range urls {
		go func(urlIn string) {
			for {
				OutgoingMessageHandler(urlIn)
				time.Sleep(time.Duration(timerMinCnt) * time.Minute)
			}
		}(url)
	}
}
