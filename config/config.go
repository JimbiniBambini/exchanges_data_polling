package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!
// MAKE ASSET MAPPING JSON CONFIG BECAUSE OF DIFFERENT PAIRS NAMING ON EXCHANGES!

/* Various Config Structs */

/* ************************************************ STORJ ************************************************ */
type AccessCredentialsStorj struct {
	sateliteKey string
	apiKey      string
	rootPhrase  string
	Version     string
}

/* Constructor */
func NewAccessCredentialsStorj() *AccessCredentialsStorj {
	type ConfInternal struct {
		SateliteKey string `json:"satelite_key"`
		ApiKey      string `json:"api_key"`
		RootPhrase  string `json:"root_phrase"`
		Version     string `json:"version"`
	}
	type ConfOuter struct {
		Config ConfInternal `json:"config"`
	}
	var confTmp ConfOuter

	absPath, _ := filepath.Abs("../config/config_storj.json")
	file, _ := os.Open(absPath)
	defer file.Close()

	fmt.Println(absPath)
	bytes, _ := ioutil.ReadAll(file)

	json.Unmarshal(bytes, &confTmp)

	return &AccessCredentialsStorj{
		sateliteKey: confTmp.Config.SateliteKey,
		apiKey:      confTmp.Config.ApiKey,
		rootPhrase:  confTmp.Config.RootPhrase,
		Version:     confTmp.Config.Version}
}

func (confStorj *AccessCredentialsStorj) GetSateliteKey() string {
	return confStorj.sateliteKey
}

func (confStorj *AccessCredentialsStorj) GetApiKey() string {
	return confStorj.apiKey
}

func (confStorj *AccessCredentialsStorj) GetRootPhrase() string {
	return confStorj.rootPhrase
}

/* ************************************************ EXCHANGES ************************************************ */
type ExchangeConfig struct {
	Api        string   `json:"api"`
	Assets     []string `json:"assets"`
	DataKey    string   `json:"data_key"`
	DataPeriod string   `json:"data_period"`
	ApiKeys    []string `json:"api_keys"`
	DataScheme []string `json:"data_scheme"`
}

func NewExchangeConfig(exchangeName string) *ExchangeConfig {
	var config ExchangeConfig
	var confTmp map[string]ExchangeConfig
	absPath, _ := filepath.Abs("../config/config_exchanges.json")
	file, _ := os.Open(absPath)
	defer file.Close()

	fmt.Println(absPath)
	bytes, _ := ioutil.ReadAll(file)

	json.Unmarshal(bytes, &confTmp)
	config = confTmp[exchangeName]
	return &config
}
