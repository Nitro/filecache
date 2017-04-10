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

	Describe("MaybeDownload()", func() {
		BeforeEach(func() {
			cache.DownloadFunc = mockDownloader
		})

		It("downloads a file that's not in the cache", func() {
			err := cache.MaybeDownload("bilbo")

			Expect(err).To(BeNil())
			Expect(didDownload).To(BeTrue())
			Expect(cache.Contains("bilbo")).To(BeTrue())
		})

		It("returns an error when the backing downloader failed", func() {
			downloadShouldError = true

			err := cache.MaybeDownload("bilbo")
			Expect(err).To(HaveOccurred())
		})

		It("does not leave garbage in 'Waiting'", func() {
			cache.MaybeDownload("bilbo")

			_, ok := cache.Waiting["bilbo"]
			Expect(ok).To(BeFalse())
		})

		It("adds entries to the cache after downloading", func() {
			Expect(cache.Contains("bilbo")).NotTo(BeTrue())

			cache.MaybeDownload("bilbo")

			Expect(cache.Contains("bilbo")).To(BeTrue())
		})

		It("doesn't duplicate a download that started already", func() {
			cache.Waiting["bilbo"] = make(chan struct{})

			// In the background we'll close/remove the channel
			// to simulate another downloader
			go func() {
				time.Sleep(1*time.Millisecond)
				close(cache.Waiting["bilbo"])
				delete(cache.Waiting, "bilbo")
			}()

			err := cache.MaybeDownload("bilbo")

			Expect(didDownload).To(BeFalse())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Fetch()", func() {
		BeforeEach(func() {
			cache.DownloadFunc = mockDownloader
		})

		It("doesn't try to download files we already have", func() {
			cache.Cache.Add("aragorn", true)

			Expect(cache.Fetch("aragorn")).To(BeTrue())
			Expect(didDownload).To(BeFalse())
		})

		It("downloads the file when we don't have it", func() {
			Expect(cache.Fetch("aragorn")).To(BeTrue())
			Expect(didDownload).To(BeTrue())
		})
	})
})
