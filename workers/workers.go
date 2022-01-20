package workers

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/JimbiniBambini/exchanges_data_polling/clients/exchanges"
	"github.com/JimbiniBambini/exchanges_data_polling/clients/storj_client"
	"github.com/JimbiniBambini/exchanges_data_polling/common"
	"github.com/JimbiniBambini/exchanges_data_polling/config"
)

/* ****************************************** WORKER IMPLEMENTATION ****************************************** */
type AssetWorker struct {
	ID       string `json:"id"`
	Asset    string `json:"asset"`
	Fiat     string `json:"fiat"`
	Exchange string `json:"exchange"`
	Period   string `json:"period"`
	Run      bool   `json:"run"`
}

func (self *AssetWorker) RunHandler(runCmd bool) {
	self.Run = runCmd
}

func (self *AssetWorker) Perform(waitTimeSec int, bucketKey string, storjClient storj_client.StorjClient, assetMapping map[string]map[string]map[string]string) {
	// 	// get latest data for exchange and asset from storj
	ctx := context.Background()

	// 	// main loop
	var cntSec int = 0
	for {
		if self.Run {
			if cntSec == 0 {
				//storjClient.GetAllBucketsAndObjects(ctx)
				storjClient.UpdateClient(ctx)
				fileLstAsset := make([]string, 0)
				for _, obj := range storjClient.Buckets[bucketKey].GetObjectList() {
					if strings.Contains(obj, self.Asset) && strings.Contains(obj, self.Fiat) && strings.Contains(obj, self.Exchange) {
						fileLstAsset = append(fileLstAsset, obj)
					}
				}
				_, idx := common.GetSortedFileNamesAndId(fileLstAsset)

				// get data from exchange
				var bytes2Upload []byte = nil

				configExchange := config.NewExchangeConfig(self.Exchange)

				log.Println(configExchange)
				log.Println(self)

				if assetExchange, ok := assetMapping[self.Asset][self.Fiat][self.Exchange]; ok {
					bytes2Upload = exchanges.GetExchangeDataCsvByte(assetExchange, *configExchange)
				}

				// upload
				newIdx := 0
				if len(idx) > 0 {
					newIdx = idx[len(idx)-1] + 1
				}
				period, _ := strconv.Atoi(configExchange.DataPeriod) // BINANCE!!!!!! --> is 1m for minute. Others may also differ!!!

				storjClient.Buckets[bucketKey].UploadObject(ctx, bytes2Upload, common.GenerateBucketObjectKey(self.Asset, self.Fiat, self.Exchange, period, newIdx), storjClient.Project)

				// --> repeat after WAIT_TIME
				log.Println("worker_id:", self.ID, "next_call_in:", waitTimeSec, "seconds")

			}
			cntSec += 1
			time.Sleep(1 * time.Second)
			if cntSec == waitTimeSec {
				cntSec = 0
			}
		} else {
			return
		}
	}
}
