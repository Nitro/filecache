package filecache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
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

// S3Download will fetch a file from the specified bucket into a localPath. It
// will create sub-directories as needed inside that path in order to store the
// complete path name of the file.
func S3Download(fname string, localPath string, bucket string, region string) error {
	if hasDirectoryComponent(localPath) {
		log.Debugf("MkdirAll() on %s", filepath.Dir(localPath))
		err := os.MkdirAll(filepath.Dir(localPath), 0755)
		if err != nil {
			return fmt.Errorf("Could not create local Directories: %s", err)
		}
	}

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("Could not create local File: %s", err)
	}

	ses, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return fmt.Errorf("Could not create S3 session: %s", err)
	}

	log.Debugf("Downloading s3://%s/%s", bucket, fname)
	startTime := time.Now()
	numBytes, err := s3manager.NewDownloader(ses).Download(
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fname),
		},
	)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("Could not fetch from S3: %s", err)
	}

	log.Debugf("Took %s to download %d from S3 for %s", time.Since(startTime), numBytes, fname)

	return nil
}
