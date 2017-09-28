package filecache_test

import (
	"errors"
	"os"
	"sync"
	"time"

	. "github.com/Nitro/filecache"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Filecache", func() {
	var (
		cache *FileCache
		err   error

		didDownload         bool
		downloadShouldSleep bool
		downloadShouldError bool
		downloadCount       int
		countLock           sync.Mutex
	)

	mockDownloader := func(fname string, url string, localPath string) error {
		if downloadShouldError {
			return errors.New("Oh no! Tragedy!")
		}
		if downloadShouldSleep {
			time.Sleep(10 * time.Millisecond)
		}
		countLock.Lock()
		downloadCount += 1
		countLock.Unlock()
		didDownload = true
		return nil
	}

	BeforeEach(func() {
		cache, err = NewS3Cache(10, ".", "aragorn-foo", "gondor-north-1")

		// Reset between runs
		didDownload = false
		downloadShouldError = false
		downloadShouldSleep = false
	})

	Describe("New()", func() {
		It("returns a properly configured instance", func() {
			cache, err := New(10, ".")

			Expect(err).To(BeNil())
			Expect(cache.Waiting).NotTo(BeNil())
			Expect(cache.Cache).NotTo(BeNil())
			Expect(cache.DownloadFunc("junk", "http://junk", "junk")).To(BeNil())
		})
	})

	Describe("NewS3Cache()", func() {
		It("returns a properly configured instance", func() {
			Expect(err).To(BeNil())
			Expect(cache.Waiting).NotTo(BeNil())
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
			cache, err = NewS3Cache(10, ".", "aragorn-foo", "gondor-north-1")
			cache.DownloadFunc = mockDownloader

			downloadCount = 0
		})

		It("downloads a file that's not in the cache", func() {
			err := cache.MaybeDownload("bilbo", "/documents/bilbo?args=asdfg")

			Expect(err).To(BeNil())
			Expect(didDownload).To(BeTrue())
			Expect(cache.Contains("bilbo")).To(BeTrue())
		})

		It("returns an error when the backing downloader failed", func() {
			downloadShouldError = true

			err := cache.MaybeDownload("bilbo", "/documents/bilbo?args=asdfg")
			Expect(err).To(HaveOccurred())
		})

		It("does not leave garbage in 'Waiting'", func() {
			cache.MaybeDownload("bilbo", "/documents/bilbo?args=asdfg")

			_, ok := cache.Waiting["bilbo"]
			Expect(ok).To(BeFalse())
		})

		It("adds entries to the cache after downloading", func() {
			Expect(cache.Contains("bilbo")).NotTo(BeTrue())

			cache.MaybeDownload("bilbo", "/documents/bilbo?args=asdfg")

			Expect(cache.Contains("bilbo")).To(BeTrue())
		})

		It("doesn't duplicate a download that started already", func() {
			// If the download doesn't take any time then we end up
			// falling back to the test case scenario "re-download on
			// a data race" below.
			downloadShouldSleep = true

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() { cache.MaybeDownload("bilbo", "/documents/bilbo?args=asdfg"); wg.Done() }()
			}
			wg.Wait()

			Expect(didDownload).To(BeTrue())
			Expect(downloadCount).To(Equal(1))
			Expect(err).NotTo(HaveOccurred())
		})

		It("doesn't re-download on a data race", func() {
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() { cache.MaybeDownload("bilbo", "/documents/bilbo?args=asdfg"); wg.Done() }()
			}
			wg.Wait()

			Expect(didDownload).To(BeTrue())
			Expect(downloadCount).To(Equal(1))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Fetch()", func() {
		BeforeEach(func() {
			cache, err = NewS3Cache(10, ".", "aragorn-foo", "gondor-north-1")
			cache.DownloadFunc = mockDownloader
		})

		It("doesn't try to download files we already have", func() {
			cache.Cache.Add("aragorn", true)

			Expect(cache.Fetch("aragorn", "/documents/aragorn?args=asdfg")).To(BeTrue())
			Expect(didDownload).To(BeFalse())
		})

		It("downloads the file when we don't have it", func() {
			Expect(cache.Fetch("aragorn", "/documents/aragorn?args=asdfg")).To(BeTrue())
			Expect(didDownload).To(BeTrue())
		})
	})

	Describe("onEvictDelete()", func() {
		BeforeEach(func() {
			cache, _ = NewS3Cache(10, ".", "aragorn-foo", "gondor-north-1")
		})

		It("calls the downstream eviction callback if it's configured", func() {
			var didRun bool

			cache.Cache.Add("test-entry", "cache-tmp")

			// We add a file here to the filesystem so we can delete it on purge
			file, err := os.Create("cache-tmp")
			Expect(err).To(BeNil())
			err = file.Close()
			Expect(err).To(BeNil())

			cache.OnEvict = func(key interface{}, value interface{}) {
				didRun = true
			}

			cache.Cache.Purge()

			Expect(didRun).To(BeTrue())
		})
	})
})
