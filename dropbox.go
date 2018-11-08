package filecache

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// DropboxDownload will download a file from the specified Dropbox location into localFile
func DropboxDownload(dr *DownloadRecord, localFile io.Writer, downloadTimeout time.Duration) error {
	// In the case of Dropbox files, the path will contain the base64-encoded file URL after dropbox/
	fileURL, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(dr.Path, "dropbox/"))

	if err != nil {
		return fmt.Errorf("could not base64 decode file URL: %s", err)
	}

	startTime := time.Now()
	ctx, cancelFunc := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancelFunc()

	req, err := http.NewRequest(http.MethodGet, string(fileURL), nil)
	if err != nil {
		return fmt.Errorf("could not create HTTP request for URL %q: %s", fileURL, err)
	}

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to download file %q: %s", fileURL, err)
	}
	defer resp.Body.Close()

	numBytes, err := io.Copy(localFile, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write local file: %s", err)
	}

	log.Debugf("Took %s to download %d bytes from Dropbox for %s", time.Since(startTime), numBytes, dr.Path)

	return nil
}
