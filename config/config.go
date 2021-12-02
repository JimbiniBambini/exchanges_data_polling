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
	absPath, _ := filepath.Abs("config/config_exchanges.json")
	file, _ := os.Open(absPath)
	defer file.Close()

	fmt.Println(absPath)
	bytes, _ := ioutil.ReadAll(file)

	json.Unmarshal(bytes, &confTmp)
	config = confTmp[exchangeName]
	return &config
}
