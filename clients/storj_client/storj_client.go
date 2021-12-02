package storj_client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"storj.io/uplink"
)

const LenID = 32
const Version = "d1.1"

const StorageOptionConcat = "concat"
const StorageOptionSingle = "single_file"
const StorjKeySeparator = "/"

type AccessCredentialsStorj struct {
	sateliteKey string
	apiKey      string
	rootPhrase  string
	version     string
}

/* Constructor */
func NewAccessCredentialsStorj(data []byte) *AccessCredentialsStorj {
	type ConfInternal struct {
		SateliteKey string `json:"satelite_key"`
		ApiKey      string `json:"api_key"`
		RootPhrase  string `json:"root_phrase"`
		Version     string `json:"version"`
	}
	var confTmp ConfInternal
	decoder := json.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&confTmp)

	if err != nil {
		panic(err)
	}

	return &AccessCredentialsStorj{
		sateliteKey: confTmp.SateliteKey,
		apiKey:      confTmp.ApiKey,
		rootPhrase:  confTmp.RootPhrase,
		version:     confTmp.Version}
}

func (credentials AccessCredentialsStorj) GetStorjClient(ctx context.Context) StorjClient {
	var client StorjClient
	if client.accessProjectMUX(ctx, credentials.sateliteKey, credentials.apiKey, credentials.rootPhrase, credentials.version) {

		client.GetAllBucketsAndObjects(ctx)

		//ChecksumIEEE([]byte(credentials.sateliteKey + credentials.apiKey + credentials.rootPhrase + credentials.version))
		// log.Println(client)
		return client
	} else {
		return StorjClient{
			Project: nil,
			Buckets: nil,
			Version: "error"}
	}
}

type StorjClient struct {
	Project *uplink.Project
	Buckets map[string]Bucket // convert to map for better indexing?????
	Version string
}

func (self *StorjClient) accessProjectMUX(ctx context.Context, sateliteKey string, apiKey string, rootPhrase string, version string) bool {
	success := true
	access, err := uplink.RequestAccessWithPassphrase(ctx, sateliteKey, apiKey, rootPhrase)
	if err != nil {
		fmt.Println(err)
		success = false
	}

	if success {
		project, err := uplink.OpenProject(ctx, access)
		if err != nil {
			fmt.Println(err)
			self.Project = nil
		} else {
			self.Project = project
		}

		self.Version = version

		defer project.Close()
	} else {
		success = false
	}
	return success
}

func (self *StorjClient) GetAllBucketsAndObjects(ctx context.Context) {
	bucketsMap := make(map[string]Bucket, 0)
	buckets := self.Project.ListBuckets(ctx, nil)
	for buckets.Next() {
		bucketTmp := Bucket{Key: buckets.Item().Name, Objects: nil}
		bucketTmp.GetAllObjects(ctx, self.Project)
		bucketsMap[buckets.Item().Name] = bucketTmp
	}

	self.Buckets = bucketsMap
}

type Bucket struct {
	Key     string
	Objects []uplink.Object // convert to map for better indexing?????
}

func (self *Bucket) GetAllObjects(ctx context.Context, project *uplink.Project) {
	buketObj := make([]uplink.Object, 0)
	objects := project.ListObjects(ctx, self.Key, project.SetOptionRecursive(true))
	for objects.Next() {
		buketObj = append(buketObj, *objects.Item())
	}
	self.Objects = buketObj
}

func (self Bucket) UploadObject(ctx context.Context, objBytes []byte, objUploadKey string, project *uplink.Project) bool {
	operationSuccess := true
	upload, err := project.UploadObject(ctx, self.Key, objUploadKey, nil)
	if err != nil {
		fmt.Errorf("could not initiate upload: %v", err)
		operationSuccess = false
	}

	buf := bytes.NewBuffer(objBytes)
	_, err = io.Copy(upload, buf)
	if err != nil {
		_ = upload.Abort()
		fmt.Errorf("could not upload data: %v", err)
		operationSuccess = false
	}

	// Commit the uploaded object.
	err = upload.Commit()
	if err != nil {
		fmt.Errorf("could not commit uploaded object: %v", err)
		operationSuccess = false
	}

	return operationSuccess
}

func (self *Bucket) DownloadObject(ctx context.Context, objUploadKey string, project *uplink.Project) ([]byte, bool) {
	operationSuccess := true
	download, err := project.DownloadObject(ctx, self.Key, objUploadKey, nil)
	if err != nil {
		fmt.Errorf("could not open object: %v", err)
		operationSuccess = false
	}
	defer download.Close()

	// Read everything from the download stream
	receivedContents, err := ioutil.ReadAll(download)
	if err != nil {
		fmt.Errorf("could not read data: %v", err)
		operationSuccess = false
	}
	return receivedContents, operationSuccess
}

func (self Bucket) GetObjectList() []string {
	objLst := make([]string, 0)
	for _, obj := range self.Objects {
		objLst = append(objLst, obj.Key)
	}
	return objLst
}

type Server struct {
	port string
}

func (self *Server) Run() {

}
