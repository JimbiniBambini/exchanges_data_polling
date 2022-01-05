package common

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func ConcatData() {

}

func ConvertCsvToJson() {

}

func ConvertJsonToCsv() {}

/* ****************************************** COMMON FUNCTION/TYPES ****************************************** */
func calcRequestBodyCheckSum(body []byte) string {
	h := sha256.New()
	r := bytes.NewReader(body)
	if _, err := io.Copy(h, r); err != nil {
		log.Fatal(err)
		return ""
	}
	var str2ret string
	for _, val := range h.Sum(nil) {
		str2ret += strconv.Itoa(int(val))
	}

	return str2ret
}

func generateRandomID(size int) string {
	rand.Seed(time.Now().UnixNano())
	min := 0
	max := 9
	var randStr string
	for i := 0; i < size; i++ {
		randStr += strconv.Itoa(rand.Intn(max-min+1) + min)
	}
	return randStr
}

func getSortedFilelistExchange(filesLst []string) []string {
	// sort numbers
	re := regexp.MustCompile(`_id_(\d+)`)
	fileNums := make([]int, 0)
	for _, val := range filesLst {
		num, _ := strconv.Atoi(re.FindStringSubmatch(val)[1])
		fileNums = append(fileNums, num)

	}

	re = regexp.MustCompile(`^(.*?)_id_`)
	fileBase := re.FindStringSubmatch(filesLst[0])[0]
	sort.Ints(fileNums)
	filesSortedLst := make([]string, 0)
	for _, val := range fileNums {
		filesSortedLst = append(filesSortedLst, fileBase+strconv.Itoa(val)+".csv")
	}
	return filesSortedLst
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

func generateBucketObjectKey(asset string, assetFiat string, exchange string, period int, objectIdx int) string {
	keyFinal := fmt.Sprintf("%s_%s_%s_period_%d_id_%d.csv", asset, assetFiat, exchange, period, objectIdx)

	return keyFinal
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
