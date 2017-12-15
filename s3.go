package filecache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

func newS3Downloader(awsRegion string) (*s3manager.Downloader, error) {
	ses, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})
	if err != nil {
		return nil, fmt.Errorf("Could not create S3 session for region '%s': %s", awsRegion, err)
	}

	return s3manager.NewDownloader(ses), nil
}

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
func S3Download(fname string, localPath string, bucket string, downloader *s3manager.Downloader, downloadTimeout time.Duration) error {
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
	defer file.Close()

	ctx, cancelFunc := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancelFunc()

	log.Debugf("Downloading s3://%s/%s", bucket, fname)
	startTime := time.Now()
	numBytes, err := downloader.DownloadWithContext(
		ctx,
		file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fname),
		},
	)

	if err != nil {
		return fmt.Errorf("Could not fetch from S3: %s", err)
	}

	log.Debugf("Took %s to download %d from S3 for %s", time.Since(startTime), numBytes, fname)

	return nil
}
