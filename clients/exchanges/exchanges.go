package exchanges

import (
	"bytes"
	"data_polling/exchanges_data_polling/config"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

/* KRAKEN */
func ConvertCsvBytes(data []byte, dir2write string) []string {
	//header := make([]string, 0)

	for idx, line := range strings.Split(string(data), "\n") {

		if idx == 0 {
			//		header = strings.Split(line, ",")
		}
		if len(line) != 0 {

		}
	}

	if len(dir2write) > 0 {
		// write the whole body at once
		err := ioutil.WriteFile(dir2write, data, 0644)
		if err != nil {
			panic(err)
		}
	}
	return strings.Split(string(data), "\n")
}
func exchangeResponseToCsv(data []byte, pair string, exchangeConfig config.ExchangeConfig) []byte {

	exchangeDataSchemeExtractor := make(map[string]map[string][][]interface{})
	json.Unmarshal(data, &exchangeDataSchemeExtractor)

	var strConcat string
	for _, valsOuter := range exchangeDataSchemeExtractor[exchangeConfig.DataKey][pair] {
		for idx, valsInner := range valsOuter {
			var s string
			switch valsInner.(type) {
			case nil:
				s = "0.0"
			case int:
				s = fmt.Sprintf("%d.0", valsInner.(int))

			case float64:
				s = fmt.Sprintf("%.1f", valsInner.(float64))
			case string:
				s = valsInner.(string)
			}

			//fmt.Println(idx, s, len(valsOuter))
			if idx < len(valsOuter)-1 {
				strConcat += (s + ",")
			} else {
				strConcat += (s + "\n")
			}
		}
	}
	//fmt.Println(strConcat)
	return []byte(strConcat)
}

func convertJsonToBytes(data []interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GetExchangeData(pair string, exchangeConfig config.ExchangeConfig) []byte {
	client := &http.Client{}

	req, err := http.NewRequest("GET", exchangeConfig.Api, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	param := url.Values{}
	for i := 0; i < len(exchangeConfig.ApiKeys); i++ {
		switch i {
		case 0:
			param.Add(exchangeConfig.ApiKeys[i], pair)
		case 1:
			param.Add(exchangeConfig.ApiKeys[i], exchangeConfig.DataPeriod)
		}
	}

	req.URL.RawQuery = param.Encode()

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Errored when sending request to the server")
		return nil
	}
	defer resp.Body.Close()

	req.URL.RawQuery = param.Encode()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	//fmt.Println(exchangeResponseToCsv(body, pair, exchangeConfig))
	return body
}

func GetExchangeDataCsvByte(pair string, exchangeConfig config.ExchangeConfig) []byte {
	body := GetExchangeData(pair, exchangeConfig)
	return exchangeResponseToCsv(body, pair, exchangeConfig)
}
