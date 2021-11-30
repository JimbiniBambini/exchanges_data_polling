package main

import (
	"context"
	"data_polling/clients/exchanges"
	"data_polling/clients/storj_client"
	"data_polling/config"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//"data_polling/clients/storj_client"
// "data_polling/config"
// "data_polling/clients/exchanges"

// exchange --> asset_regular --> fiat --> exchange_specific_asset
var assetMapping map[string]map[string]map[string]string

func routines() {

}

func generateBucketObjectKey(asset string, assetFiat string, exchange string, period int, objectIdx int) string {
	keyFinal := fmt.Sprintf("%s_%s_%s_period_%d_id_%d.csv", asset, assetFiat, exchange, period, objectIdx)

	return keyFinal
}

func makeInitialSingleFileUpload(ctx context.Context, self *storj_client.StorjClient, asset string, fiat string, exchange string, period int, path2Folder string) {
	// ensure bucket --> Version + Pair + Option + Exchange
	var keys []string = []string{self.Version, asset, exchange, storj_client.StorageOptionSingle}

	_, err := self.Project.EnsureBucket(ctx, keys[0])
	if err != nil {
		fmt.Errorf("could not ensure bucket: %v", err)
	}
	self.GetAllBucketsAndObjects(ctx)
	files, err := ioutil.ReadDir(path2Folder)
	if err != nil {
		log.Fatal(err)
	}

	var fileNumbers []int
	var fileNames []string
	var fileNameCut []string

	for _, f := range files {
		if strings.Contains(f.Name(), ".csv") {
			//fmt.Println(f.Name(), strings.Contains(f.Name(), ".csv"))
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

	for _, val := range self.Buckets {
		if val.Key == self.Version {
			for idx, fileName := range fileNamesSorted {
				fmt.Println("Uploading file:", idx+1, "Total Files:", len(fileNamesSorted))
				b, err := ioutil.ReadFile(path2Folder + string(filepath.Separator) + fileName)
				if err != nil {
					fmt.Print(err)
				}
				fmt.Println(generateBucketObjectKey(asset, fiat, exchange, period, idx))
				val.UploadObject(ctx, b, generateBucketObjectKey(asset, fiat, exchange, period, idx), self.Project)

				//fmt.Println(b)
			}
		}
	}

}

func getSortedFileNamesAndId(files []string) ([]string, []int) {
	var fileNumbers []int
	var fileNames []string
	var fileNameCut []string
	for _, f := range files {
		if strings.Contains(f, ".csv") {
			//fmt.Println(f.Name(), strings.Contains(f.Name(), ".csv"))
			fileNameCut = strings.Split(strings.Split(f, ".csv")[0], "_")
			num, _ := strconv.Atoi(fileNameCut[len(fileNameCut)-1])
			fileNumbers = append(fileNumbers, num)
			fileNames = append(fileNames, f)
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

	return fileNamesSorted, fileNumbers
}

func getFileNameStructure(fileNameIn string) (string, string) {
	// Return vars
	var fileExtension string
	var baseStructure string

	// Impl
	stringConstructor := strings.Split(fileNameIn, ".")
	fileExtension = "." + stringConstructor[len(stringConstructor)-1]
	var baseNameConstructor string = strings.Split(fileNameIn, fileExtension)[0]
	fileNameStructureSplit := strings.Split(baseNameConstructor, "_")
	for _, val := range fileNameStructureSplit[:len(fileNameStructureSplit)-1] {
		baseStructure += (val + "_")
	}

	return baseStructure, fileExtension
}

// workers
func exchangeWorker() {

}

func assetWorker(asset string, fiat string, exchange string) {
	// get latest data for exchange and asset from storj
	var storjClient storj_client.StorjClient
	ctx := context.Background()
	storjClient.AccessProject(ctx, config.NewAccessCredentialsStorj())

	// main loop
	for {
		storjClient.GetAllBucketsAndObjects(ctx)
		fileLstAsset := make([]string, 0)
		for _, obj := range storjClient.Buckets[storjClient.Version].GetObjectList() {
			fmt.Println(obj)
			if strings.Contains(obj, asset) && strings.Contains(obj, fiat) && strings.Contains(obj, exchange) {
				fileLstAsset = append(fileLstAsset, obj)
			}
		}
		fileNames, idx := getSortedFileNamesAndId(fileLstAsset)
		fmt.Println(fileNames, idx)
		// fileNameStructure, fileExtension := getFileNameStructure(fileNames[len(fileNames)-1])
		// newFileName := fileNameStructure + strconv.Itoa(ids[len(ids)-1]+1) + fileExtension
		// fmt.Println(newFileName)
		// get data from exchange
		var bytes2Upload []byte = nil

		configExchange := config.NewExchangeConfig(exchange)
		for _, assetExchange := range configExchange.Assets {
			if assetExchange == assetMapping[exchange][asset][fiat] {
				bytes2Upload = exchanges.GetExchangeDataCsvByte(assetExchange, *configExchange)
			}
		}

		// upload
		period, _ := strconv.Atoi(configExchange.DataPeriod) // BINANCE!!!!!! --> is 1m for minute. Others may also differ!!!
		fmt.Println(generateBucketObjectKey(asset, fiat, exchange, period, idx[len(idx)-1]+1))
		storjClient.Buckets[storjClient.Version].UploadObject(ctx, bytes2Upload, generateBucketObjectKey(asset, fiat, exchange, period, idx[len(idx)-1]+1), storjClient.Project)
		// --> repeat after WAIT_TIME
		time.Sleep(60 * time.Second)
	}
}

func main() {
	assetMapping = make(map[string]map[string]map[string]string)
	assetMapping["kraken"] = make(map[string]map[string]string)
	assetMapping["kraken"]["btc"] = make(map[string]string)
	assetMapping["kraken"]["btc"]["usd"] = "XXBTZUSD"

	var storjClient storj_client.StorjClient
	ctx := context.Background()
	storjClient.AccessProject(ctx, config.NewAccessCredentialsStorj())
	storjClient.GetAllBucketsAndObjects(ctx)

	// NOOOOOOOO!!!!!!!!!
	//makeInitialSingleFileUpload(ctx, &storjClient, "btc", "usd", "kraken", 1, "/Users/Slava/SW_Projects/git/PrivateProjects/trading_pattern_bot/csv_data/exchange_api_data/kraken/1/xbtusd")
	// NOOOOOOOO!!!!!!!!!

	assetWorker("btc", "usd", "kraken")

	//bucketNr := 0
	//dataDownload, _ := storjClient.Buckets[bucketNr].DownloadObject(ctx, "btc_usd_kraken_period_1_id_0.csv", storjClient.Project)
	//	fmt.Println(len(storjClient.Buckets[bucketNr].Objects), strings.Split(strings.Split(string(dataDownload), "\n")[0], ","))
	//fmt.Println(dataDownload)

	// configKraken := config.NewExchangeConfig("kraken")
	// for id, asset := range configKraken.Assets {
	// 	if id == 0 {
	// 		fmt.Println(asset)
	// 		//exchanges.GetExchangeData(asset, *configKraken)
	// 		fmt.Println(exchanges.GetExchangeDataCsvByte(asset, *configKraken))
	// 	}

	// }

}
