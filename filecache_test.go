package filecache_test

import (
	"errors"
	"time"

	. "github.com/Nitro/filecache"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Filecache", func() {
	var cache *FileCache
	var err error
	var didDownload, downloadShouldSleep, downloadShouldError bool

	mockDownloader := func(fname string, localPath string) error {
		if downloadShouldError {
			return errors.New("Oh no! Tragedy!")
		}
		if downloadShouldSleep {
			time.Sleep(10 * time.Millisecond)
		}
		didDownload = true
		return nil
	}

	BeforeEach(func() {
		cache, err = New(10, ".", "aragorn-foo", "gondor-north-1")

		// Reset between runs
		didDownload = false
		downloadShouldError = false
		downloadShouldSleep = false
	})

	Describe("New()", func() {
		It("returns a properly configured instance", func() {
			Expect(err).To(BeNil())
			Expect(cache.Waiting).NotTo(BeNil())
			Expect(cache.AwsRegion).To(Equal("gondor-north-1"))
			Expect(cache.S3Bucket).To(Equal("aragorn-foo"))
			Expect(cache.Cache).NotTo(BeNil())
			Expect(cache.DownloadFunc).NotTo(BeNil())
		})
	})

	Describe("Contains()", func() {
		It("identifies keys that are not present", func() {
			Expect(cache.Contains("gandalf")).To(BeFalse())
		})

		It("identifies keys that are  present", func() {
			cache.Cache.Add("gandalf", true)
			Expect(cache.Contains("gandalf")).To(BeTrue())
		})
	})

	Describe("Download()", func() {
		BeforeEach(func() {
			cache.DownloadFunc = mockDownloader
		})

		It("skips downloading when we have the file", func() {
			cache.Cache.Add("bilbo", true)
			err := cache.Download("bilbo")

			Expect(err).To(BeNil())
			Expect(didDownload).To(BeFalse())
		})

		It("downloads a file that's not in the cache", func() {
			err := cache.Download("bilbo")

			Expect(err).To(BeNil())
			Expect(didDownload).To(BeTrue())
			Expect(cache.Contains("bilbo")).To(BeTrue())
		})

		It("returns an error when the backing downloader failed", func() {
			downloadShouldError = true

			err := cache.Download("bilbo")
			Expect(err).To(HaveOccurred())
		})

		It("does not leave garbage in 'Waiting'", func() {
			cache.Download("bilbo")

			_, ok := cache.Waiting["bilbo"]
			Expect(ok).To(BeFalse())
		})

		It("adds entries to the cache after downloading", func() {
			cache.Download("bilbo")

			Expect(cache.Contains("bilbo")).To(BeTrue())
		})
	})
})
