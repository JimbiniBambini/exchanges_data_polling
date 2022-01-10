package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"

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

func extractBetweenQuotes(strIn string) string {
	re := regexp.MustCompile(`"(.*?)"`)
	newStr := re.FindAllString(strIn, -1)[0]

	return newStr[1 : len(newStr)-1]
}
func getClientId(w http.ResponseWriter, r *http.Request) string {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("Error while reading request body")
	}

	var reqHandler LoginCredentialsIf
	decoder := json.NewDecoder(bytes.NewReader(body))
	err = decoder.Decode(&reqHandler)

	url := "http://127.0.0.1:8088/storj_client_manager"

	idGetter := sendReq(url, reqHandler, "POST")
	strResp := extractBetweenQuotes(string(idGetter))
	if strResp == "error" {

		strResp = extractBetweenQuotes(string(sendReq(url, Commander{Command: "list_clients"}, "GET")))
	}

	return strResp
}

func routineIf(w http.ResponseWriter, r *http.Request) {
	clientId := getClientId(w, r)

	log.Println(clientId)
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		routineIf(w, r)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8089" // Default port if not specified
	}

	http.ListenAndServe(":"+port, r)
}
