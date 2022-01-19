package storj_client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"storj.io/uplink"
)

const LenID = 32

const StorageOptionConcat = "concat"
const StorageOptionSingle = "single_file"
const StorjKeySeparator = "/"

type AccessCredentialsStorj struct {
	sateliteKey string
	apiKey      string
	rootPhrase  string
}

type StorjClient struct {
	accessData AccessCredentialsStorj
	Project    *uplink.Project
	Buckets    map[string]Bucket
	//Version string
}

// Constructor with direct access to storj client and all buckets with stored there elements
func NewStorjClient(ctx context.Context, credentialsIn map[string]string) (StorjClient, bool) {
	var newClient StorjClient
	newClient.setAccessData(ctx, credentialsIn)
	if newClient.accessProjectMUX(ctx) {
		newClient.GetAllBucketsAndObjects(ctx)
		return newClient, true
	} else {
		return StorjClient{
			accessData: AccessCredentialsStorj{},
			Project:    nil,
			Buckets:    nil}, false
	}
}

func (self *StorjClient) UpdateClient(ctx context.Context) {
	if self.accessProjectMUX(ctx) {
		self.GetAllBucketsAndObjects(ctx)

	}

}

func (self *StorjClient) setAccessData(ctx context.Context, credentialsIn map[string]string) {
	self.accessData.sateliteKey = credentialsIn["satelite_key"]
	self.accessData.apiKey = credentialsIn["api_key"]
	self.accessData.rootPhrase = credentialsIn["root_phrase"]
}

func (self *StorjClient) accessProjectMUX(ctx context.Context) bool {
	success := true
	access, err := uplink.RequestAccessWithPassphrase(ctx, self.accessData.sateliteKey, self.accessData.apiKey, self.accessData.rootPhrase)
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

		//self.Version = version

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

func (self *StorjClient) DeleteBucket(ctx context.Context, bucketKey string, delBucketAndFiles bool) bool {
	var err error

	if delBucketAndFiles {
		_, err = self.Project.DeleteBucketWithObjects(ctx, bucketKey)
	} else {
		_, err = self.Project.DeleteBucket(ctx, bucketKey)
	}

	if err == nil {
		return true
	} else {
		return false
	}
}

func (self *StorjClient) AddBucket(ctx context.Context, bucketKey string) bool {
	_, err := self.Project.CreateBucket(ctx, bucketKey)

	if err == nil {
		return true
	} else {
		return false
	}
}

type Bucket struct {
	Key     string
	Objects []uplink.Object // convert to map for better indexing?????
}

func (self *Bucket) GetAllObjects(ctx context.Context, project *uplink.Project) {
	buketObj := make([]uplink.Object, 0)

	objects := project.ListObjects(ctx, self.Key, &uplink.ListObjectsOptions{Recursive: true})
	//objects := project.ListObjects(ctx, self.Key, project.SetOptionRecursive(true))
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

func (self Bucket) DownloadObject(ctx context.Context, objUploadKey string, project *uplink.Project) ([]byte, bool) {
	operationSuccess := true
	receivedContents := make([]byte, 0)

	download, err := project.DownloadObject(ctx, self.Key, objUploadKey, nil)

	if err != nil {
		log.Println("could not open object: %v", err)
		operationSuccess = false
	}

	if operationSuccess {
		// Read everything from the download stream
		receivedContents, err = ioutil.ReadAll(download)
		if err != nil {
			fmt.Errorf("could not read data: %v", err)
			operationSuccess = false
		}
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

func (self Bucket) GetFilteredObjectList(filters []string) []string {
	objLst := make([]string, 0)

	for _, object := range self.Objects {
		filtCnt := 0
		for _, filt := range filters {

			if strings.Contains(object.Key, filt) {
				filtCnt += 1
				//log.Println(filt, object.Key, filtCnt)
			}
		}

		if filtCnt == len(filters) || len(filters) == 0 {
			objLst = append(objLst, object.Key)
		}
	}
	//log.Println(objLst)
	return objLst
}
