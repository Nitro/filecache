package filecache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func hasDirectoryComponent(localPath string) bool {
	parts := strings.Split(localPath, "/")
	if len(parts) == 2 && parts[0][0] == '.' {
		return false
	}
	return len(parts) > 1
}

func (c *FileCache) S3Download(fname string, localPath string) (file *os.File, numBytes int64, err error) {
	if hasDirectoryComponent(localPath) {
		log.Debugf("MkdirAll() on %s", filepath.Dir(localPath))
		err = os.MkdirAll(filepath.Dir(localPath), 0755)
		if err != nil {
			return nil, 0, fmt.Errorf("Could not create local Directories: %s", err)
		}
	}

	file, err = os.Create(localPath)
	if err != nil {
		return nil, 0, fmt.Errorf("Could not create local File: %s", err)
	}

	log.Debugf("Downloading s3://%s/%s", c.S3Bucket, fname)
	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(c.AwsRegion)}))
	startTime := time.Now().UTC()
	numBytes, err = downloader.Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(c.S3Bucket),
			Key:    aws.String(fname),
		},
	)
	if err != nil {
		return nil, 0, fmt.Errorf("Could not fetch from S3: %s", err)
	}

	log.Debugf("Took %s to download %d from S3 for %s", time.Now().UTC().Sub(startTime), numBytes, fname)

	return file, numBytes, nil
}
