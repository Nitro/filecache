package filecache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"
)

// Manages a cache of s3manager.Downloader s that have been configured
// for their correct region.
type S3RegionManagedDownloader struct {
	sync.RWMutex
	DefaultRegion   string
	DownloaderCache map[string]*s3manager.Downloader // Map buckets to regions
}

// NewS3RegionManagedDownloader returns a configured instance where the default
// bucket region will be as passed. This means bucket lookups from the cache
// will prefer that region when hinting to S3 which region they believe a bucket
// lives in.
func NewS3RegionManagedDownloader(defaultRegion string) *S3RegionManagedDownloader {
	return &S3RegionManagedDownloader{
		DefaultRegion:   defaultRegion,
		DownloaderCache: make(map[string]*s3manager.Downloader),
	}
}

// GetDownloader looks up a bucket in the cache and returns a configured
// s3manager.Downloader for it or provisions a new one and returns that.
// NOTE! This is never flushed and so should not be used with an unlimited
// number of buckets! The first few requests will incur an additional
// penalty of roundtrips to Amazon to look up the region fo the requested
// S3 bucket.
func (m *S3RegionManagedDownloader) GetDownloader(ctx context.Context, bucket string) (*s3manager.Downloader, error) {

	m.RLock()
	// Look it up in the cache first
	if dLoader, ok := m.DownloaderCache[bucket]; ok {
		m.RUnlock()
		return dLoader, nil
	}
	m.RUnlock()

	// We need an arbitrary, region-less session
	sess := session.Must(session.NewSession())

	region, err := s3manager.GetBucketRegion(ctx, sess, bucket, m.DefaultRegion)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return nil, fmt.Errorf("Region for %s not found", bucket)
		}
		return nil, err
	}
	log.Debugf("Bucket '%s' is in region: %s", bucket, region)

	sess, err = session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, fmt.Errorf("Could not create S3 session for region '%s': %s", region, err)
	}

	// Configure and then cache the downloader
	dLoader := s3manager.NewDownloader(sess)
	m.Lock()
	m.DownloaderCache[bucket] = dLoader
	m.Unlock()

	return dLoader, nil
}

// Download will download a file from the specified S3 bucket into localFile
func (m *S3RegionManagedDownloader) Download(dr *DownloadRecord, localFile *os.File, downloadTimeout time.Duration) error {
	fname := dr.Path

	// The S3 bucket is the first part of the path, everything else is filename
	parts := strings.Split(fname, "/")
	if len(parts) < 2 {
		return fmt.Errorf("Not enough path to fetch a file! Expected <bucket>/<filename>")
	}
	bucket := parts[0]
	fname = strings.Join(parts[1:], "/")

	ctx, cancelFunc := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancelFunc()

	log.Debugf("Getting downloader for %s", bucket)
	downloader, err := m.GetDownloader(ctx, bucket)
	if err != nil {
		return fmt.Errorf("Unable to get downloader for %s: %s", bucket, err)
	}

	var requestID, hostID string
	requestInspectorFunc := func(r *request.Request) {
		r.Handlers.Complete.PushBack(func(req *request.Request) {
			requestID = req.RequestID
			if req.HTTPResponse != nil && req.HTTPResponse.Header != nil {
				hostID = req.HTTPResponse.Header.Get("X-Amz-Id-2")
			}
		})
	}

	startTime := time.Now()
	numBytes, err := downloader.DownloadWithContext(
		ctx,
		localFile,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fname),
		},
		s3manager.WithDownloaderRequestOptions(
			requestInspectorFunc,
		),
	)
	if err != nil {
		errMessage := err.Error()
		if s3Err, ok := err.(s3.RequestFailure); ok {
			errMessage = fmt.Sprintf(
				"Request ID %q on host %q failed: %s", s3Err.RequestID(), s3Err.HostID(), errMessage,
			)
		}
		return fmt.Errorf("Could not fetch from S3: %s", errMessage)
	}

	log.Infof(
		"Took %.2fms to download s3://%s/%s (%d bytes) with request ID %q and host ID %q",
		time.Since(startTime).Seconds()*1000, bucket, fname, numBytes, requestID, hostID,
	)

	if numBytes < 1 {
		return errors.New("0 length file received from S3")
	}

	return nil
}
