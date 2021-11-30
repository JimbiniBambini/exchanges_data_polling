package storj_client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"data_polling/config"

	"storj.io/uplink"
)

const StorageOptionConcat = "concat"
const StorageOptionSingle = "single_file"
const StorjKeySeparator = "/"

type StorjClient struct {
	Project *uplink.Project
	Buckets map[string]Bucket // convert to map for better indexing?????
	Version string
}

func (self *StorjClient) AccessProject(ctx context.Context, accessData *config.AccessCredentialsStorj) {

	access, err := uplink.RequestAccessWithPassphrase(ctx, accessData.GetSateliteKey(), accessData.GetApiKey(), accessData.GetRootPhrase())
	if err != nil {
		fmt.Println(err)
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		fmt.Println(err)
		self.Project = nil
	} else {
		self.Project = project
	}

	self.Version = accessData.Version

	defer project.Close()
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
