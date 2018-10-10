package filecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

var (
	dropboxAccessToken = strings.ToLower("DropboxAccessToken")
)

// DropboxDownload will download a file from the specified Dropbox location into localFile
func DropboxDownload(dr *DownloadRecord, localFile *os.File, downloadTimeout time.Duration) error {
	accessToken := dr.Args[dropboxAccessToken]
	if accessToken == "" {
		return fmt.Errorf("missing %q header", dropboxAccessToken)
	}

	// The actual path of the file should be after the "dropbox" prefix
	if !strings.HasPrefix(dr.Path, "dropbox/") {
		return errors.New("missing dropbox prefix in file path")
	}

	// In the case of Dropbox files, the path will contain the file ID
	fileID := "id:" + strings.TrimLeft(dr.Path, "dropbox/")

	// Ripped off from here https://github.com/dropbox/dropbox-sdk-go-unofficial/blob/7afa861bfde5a348d765522b303b6fbd9d250155/dropbox/sdk.go#L153-L157
	// because we have to set the `Client` field manually in `dropbox.Config` if we want to configure
	// a custom timeout :(
	conf := &oauth2.Config{Endpoint: dropbox.OAuthEndpoint(".dropboxapi.com")}
	tok := &oauth2.Token{AccessToken: accessToken}
	client := conf.Client(context.Background(), tok)
	client.Timeout = downloadTimeout

	dbx := files.New(
		dropbox.Config{
			Token:  accessToken,
			Client: client,
			// Enable Dropbox logging if needed
			// LogLevel: dropbox.LogInfo,
		},
	)

	startTime := time.Now()
	_, content, err := dbx.Download(files.NewDownloadArg(fileID))
	if err != nil {
		return fmt.Errorf("could not download file: %s", err)
	}
	defer content.Close()

	numBytes, err := io.Copy(localFile, content)
	if err != nil {
		return fmt.Errorf("failed to write local file: %s", err)
	}

	log.Debugf("Took %s to download %d bytes from Dropbox for %s", time.Since(startTime), numBytes, dr.Path)

	return nil
}
