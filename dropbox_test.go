package filecache_test

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/Nitro/filecache"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type dummyWriter struct {
	receivedData string
	writeError   error
}

func (dw *dummyWriter) Write(p []byte) (n int, err error) {
	if dw.writeError != nil {
		return 0, dw.writeError
	}

	dw.receivedData = string(p)
	return len(p), nil
}

var _ = Describe("DropboxDownload", func() {
	It("downloads a file successfully", func() {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("dummy_content"))
			Expect(err).To(BeNil())
		}))
		defer ts.Close()
		url := fmt.Sprintf(
			"dropbox/%s",
			base64.StdEncoding.EncodeToString([]byte(ts.URL)),
		)

		dr, err := NewDownloadRecord(url, nil)
		Expect(err).To(BeNil())

		writer := &dummyWriter{}
		err = DropboxDownload(dr, writer, 100*time.Millisecond)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(writer.receivedData).To(ContainSubstring("dummy_content"))
	})

	It("fails to decode an invalid base64-encoded Dropbox URL", func() {
		dr, err := NewDownloadRecord("dropbox/foo.bar", nil)
		Expect(err).To(BeNil())

		err = DropboxDownload(dr, &dummyWriter{}, 100*time.Millisecond)
		Expect(err).Should(HaveOccurred())
	})

	It("fails to create a HTTP request for an invalid URL", func() {
		url := fmt.Sprintf(
			"dropbox/%s",
			base64.StdEncoding.EncodeToString([]byte("ht$tp://invalid_url")),
		)

		dr, err := NewDownloadRecord(url, nil)
		Expect(err).To(BeNil())

		err = DropboxDownload(dr, &dummyWriter{}, 100*time.Millisecond)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error when trying to download from an unreachable domain", func() {
		url := fmt.Sprintf(
			"dropbox/%s",
			base64.StdEncoding.EncodeToString([]byte("http://some_dummy_domain.com")),
		)

		dr, err := NewDownloadRecord(url, nil)
		Expect(err).To(BeNil())

		err = DropboxDownload(dr, &dummyWriter{}, 100*time.Millisecond)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error when streaming the file to disk fails", func() {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("dummy_content"))
			Expect(err).To(BeNil())
		}))
		defer ts.Close()
		url := fmt.Sprintf(
			"dropbox/%s",
			base64.StdEncoding.EncodeToString([]byte(ts.URL)),
		)

		dr, err := NewDownloadRecord(url, nil)
		Expect(err).To(BeNil())

		writer := &dummyWriter{writeError: errors.New("dummy_error")}
		err = DropboxDownload(dr, writer, 100*time.Millisecond)
		Expect(err).Should(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dummy_error"))
	})

	It("fails to download when timing out", func() {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("dummy_content"))
			Expect(err).To(BeNil())
		}))
		defer ts.Close()
		url := fmt.Sprintf(
			"dropbox/%s",
			base64.StdEncoding.EncodeToString([]byte(ts.URL)),
		)

		dr, err := NewDownloadRecord(url, nil)
		Expect(err).To(BeNil())

		writer := &dummyWriter{}
		err = DropboxDownload(dr, writer, 0*time.Millisecond)
		Expect(err).Should(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("context deadline exceeded"))
	})
})
