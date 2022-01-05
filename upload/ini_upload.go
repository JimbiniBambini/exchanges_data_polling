package main

import (
	"exchanges_data_polling/storj_client"

	"fmt"
)

/*
func makeInitialSingleFileUpload(ctx context.Context, self *exchanges_data_polling.storj_client.StorjClient, bucketKey string, asset string, fiat string, exchange string, period int, path2Folder string) {
	// ensure bucket --> Version + Pair + Option + Exchange
	var keys []string = []string{bucketKey, asset, exchange, exchanges_data_polling.storj_client.StorageOptionSingle}

	_, err := self.Project.EnsureBucket(ctx, bucketKey)
	if err != nil {
		fmt.Errorf("could not ensure bucket: %v", err)
	}

	self.UpdateClient(ctx)

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

	for idx, fileName := range fileNamesSorted {
		fmt.Println("Uploading file:", idx+1, "Total Files:", len(fileNamesSorted))
		b, err := ioutil.ReadFile(path2Folder + string(filepath.Separator) + fileName)
		if err != nil {
			fmt.Print(err)
		}
		fmt.Println(common.GenerateBucketObjectKey(asset, fiat, exchange, period, idx))
		self.Buckets[bucketKey].UploadObject(ctx, b, common.GenerateBucketObjectKey(asset, fiat, exchange, period, idx), self.Project)
		//fmt.Println(b)
	}

}
*/
func main() {
	fmt.Println("Test")
	tst := storj_client.StorjClient{}
	fmt.Println(tst)
}
