package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

/* Various Config Structs */

/* ************************************************ STORJ ************************************************ */

/* ************************************************ EXCHANGES ************************************************ */
type ExchangeConfig struct {
	Api        string   `json:"api"`
	DataKey    string   `json:"data_key"`
	DataPeriod string   `json:"data_period"`
	ApiKeys    []string `json:"api_keys"`
	DataScheme []string `json:"data_scheme"`
}

func NewExchangeConfig(exchangeName string) *ExchangeConfig {
	var config ExchangeConfig
	var confTmp map[string]ExchangeConfig
	absPath, _ := filepath.Abs(os.Getenv("EXCHANGE_CONFIG_PATH"))
	log.Println(absPath)
	file, _ := os.Open(absPath)
	defer file.Close()

	bytes, _ := ioutil.ReadAll(file)

	json.Unmarshal(bytes, &confTmp)
	config = confTmp[exchangeName]

	return &config
}

type AssetExchangeMap struct {
	Exchange   string `json:"exchange"`
	MappedName string `json:"mapped_name"`
}

type AssetMap struct {
	Name    string             `json:"name"`
	Fiat    string             `json:"fiat"`
	Mapping []AssetExchangeMap `json:"mapping"`
}

type AssetMapping struct {
	AssetMaps []AssetMap `json:"assets"`
}

// asset --> fiat --> exchange --> exchange_specific_asset
func GetAssetConfigMap() map[string]map[string]map[string]string {
	assetMapFinal := make(map[string]map[string]map[string]string)

	var assetMappingConf AssetMapping
	absPath, _ := filepath.Abs(os.Getenv("ASSET_CONFIG_PATH"))
	file, _ := os.Open(absPath)
	defer file.Close()

	bytes, _ := ioutil.ReadAll(file)

	json.Unmarshal(bytes, &assetMappingConf)

	for _, assetMapTmp := range assetMappingConf.AssetMaps {
		assetMapFinal[assetMapTmp.Name] = make(map[string]map[string]string)
		assetMapFinal[assetMapTmp.Name][assetMapTmp.Fiat] = make(map[string]string)
		for _, exchangeMap := range assetMapTmp.Mapping {
			assetMapFinal[assetMapTmp.Name][assetMapTmp.Fiat][exchangeMap.Exchange] = exchangeMap.MappedName
		}
	}

	return assetMapFinal
}
