package filecache

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
		cacheFile           string
		s3FilePath          = "/documents/test-bucket/foo.bar"
		dropboxFilePath     = "/documents/dropbox/foo.bar"
		dropboxAccessToken  = strings.ToLower("DropboxAccessToken")
	)

	mockDownloader := func(dr *DownloadRecord, localPath string) error {
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

	// Set the dummy dropboxAccessToken in the global HashableArgs map
	HashableArgs[dropboxAccessToken] = struct{}{}

	BeforeEach(func() {
		cache, err = New(10, ".", DownloadTimeout(1*time.Millisecond), S3Downloader("gondor-north-1"))
		Expect(err).To(BeNil())

		// Reset between runs
		didDownload = false
		downloadShouldError = false
		downloadShouldSleep = false
	})

	Describe("New()", func() {
		BeforeEach(func() {
			cache, err = New(10, ".")
			Expect(err).To(BeNil())
		})
		It("returns a properly configured instance", func() {
			Expect(cache.Waiting).NotTo(BeNil())
			Expect(cache.Cache).NotTo(BeNil())
			Expect(cache.Cache.Len()).To(Equal(0))
			Expect(cache.BaseDir).To(Equal("."))
		})

		It("fails to download stuff", func() {
			Expect(cache.DownloadFunc(&DownloadRecord{Path: "junk"}, "junk")).Should(Not(Succeed()))
		})
	})

	Describe("New() with S3Downloader and DropboxDownloader", func() {
		It("returns a properly configured instance", func() {
			cache, err = New(10, ".", S3Downloader("gondor-north-1"), DropboxDownloader())
			Expect(err).To(BeNil())
			Expect(cache.downloaders[DownloadMangerS3]).To(Not(BeNil()))
			Expect(cache.downloaders[DownloadMangerDropbox]).To(Not(BeNil()))
		})
	})

	Describe("Contains()", func() {
		It("identifies keys that are not present", func() {
			Expect(cache.Contains(&DownloadRecord{Path: "gandalf"})).To(BeFalse())
		})

		It("identifies keys that are  present", func() {
			cache.Cache.Add("gandalf", true)
			Expect(cache.Contains(&DownloadRecord{Path: "gandalf"})).To(BeTrue())
		})
	})

	Describe("MaybeDownload()", func() {
		BeforeEach(func() {
			cache, err = New(10, ".", S3Downloader("gondor-north-1"), DownloadTimeout(1*time.Millisecond))
			Expect(err).To(BeNil())
			cache.DownloadFunc = mockDownloader

			downloadCount = 0
		})

		It("downloads a file that's not in the cache", func() {
			err = cache.MaybeDownload(&DownloadRecord{Path: "bilbo"})

			Expect(err).To(BeNil())
			Expect(didDownload).To(BeTrue())
			Expect(cache.Contains(&DownloadRecord{Path: "bilbo"})).To(BeTrue())
		})

		It("returns an error when the backing downloader failed", func() {
			downloadShouldError = true

			err = cache.MaybeDownload(&DownloadRecord{Path: "bilbo"})
			Expect(err).To(HaveOccurred())
		})

		It("does not leave garbage in 'Waiting'", func() {
			cache.MaybeDownload(&DownloadRecord{Path: "bilbo"})

			_, ok := cache.Waiting["bilbo"]
			Expect(ok).To(BeFalse())
		})

		It("adds entries to the cache after downloading", func() {
			Expect(cache.Contains(&DownloadRecord{Path: "bilbo"})).NotTo(BeTrue())

			cache.MaybeDownload(&DownloadRecord{Path: "bilbo"})

			Expect(cache.Contains(&DownloadRecord{Path: "bilbo"})).To(BeTrue())
		})

		It("doesn't duplicate a download that started already", func() {
			// If the download doesn't take any time then we end up
			// falling back to the test case scenario "re-download on
			// a data race" below.
			downloadShouldSleep = true

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() { cache.MaybeDownload(&DownloadRecord{Path: "bilbo"}); wg.Done() }()
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
				go func() { cache.MaybeDownload(&DownloadRecord{Path: "bilbo"}); wg.Done() }()
			}
			wg.Wait()

			Expect(didDownload).To(BeTrue())
			Expect(downloadCount).To(Equal(1))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Fetch()", func() {
		BeforeEach(func() {
			cache, err = New(10, ".", S3Downloader("gondor-north-1"), DownloadTimeout(1*time.Millisecond))
			cache.DownloadFunc = mockDownloader
			didDownload = false
		})

		It("doesn't try to download files we already have", func() {
			cache.Cache.Add("aragorn", true)

			Expect(cache.Fetch(&DownloadRecord{Path: "aragorn"})).To(BeTrue())
			Expect(didDownload).To(BeFalse())
		})

		It("downloads the file when we don't have it", func() {
			Expect(cache.Fetch(&DownloadRecord{Path: "aragorn"})).To(BeTrue())
			Expect(didDownload).To(BeTrue())
		})

		It("downloads a new file for records with the same path but different args", func() {
			args := map[string]string{
				dropboxAccessToken: "KnockKnock",
			}

			fooRec, _ := NewDownloadRecord(s3FilePath, args)
			Expect(cache.Fetch(fooRec)).To(BeTrue())
			Expect(didDownload).To(BeTrue())

			// It should be in the cache now
			didDownload = false
			Expect(cache.Fetch(fooRec)).To(BeTrue())
			Expect(didDownload).To(BeFalse())

			// Using different args should create a new cache entry
			didDownload = false
			args[dropboxAccessToken] = "ComeIn"
			fooRec, _ = NewDownloadRecord(dropboxFilePath, args)
			Expect(cache.Fetch(fooRec)).To(BeTrue())
			Expect(didDownload).To(BeTrue())
		})
	})

	Describe("FetchNewerThan()", func() {
		BeforeEach(func() {
			cache, err = New(10, os.TempDir(), S3Downloader("gondor-north-1"), DownloadTimeout(1*time.Millisecond))
			cache.DownloadFunc = mockDownloader
			didDownload = false

			// Manually write the file to the cache
			cacheFile = filepath.Join(os.TempDir(), cache.GetFileName(&DownloadRecord{Path: "aragorn"}))
			os.MkdirAll(filepath.Dir(cacheFile), 0755)
			ioutil.WriteFile(cacheFile, []byte(`some bytes`), 0644)
		})

		AfterEach(func() {
			os.RemoveAll(cacheFile)
		})

		It("doesn't try to download files we already have if they are new enough", func() {
			cache.Cache.Add("aragorn", cache.GetFileName(&DownloadRecord{Path: "aragorn"}))
			os.MkdirAll(filepath.Dir(cache.GetFileName(&DownloadRecord{Path: "aragorn"})), 0755)
			ioutil.WriteFile(cache.GetFileName(&DownloadRecord{Path: "aragorn"}), []byte("aragorn"), 0644)

			Expect(cache.FetchNewerThan(&DownloadRecord{Path: "aragorn"}, time.Now().Add(-10*time.Minute))).To(BeTrue())
			Expect(didDownload).To(BeFalse())
		})

		It("downloads the file when it's too old", func() {
			cache.Cache.Add("aragorn", cache.GetFileName(&DownloadRecord{Path: "aragorn"}))
			Expect(cache.FetchNewerThan(&DownloadRecord{Path: "aragorn"}, time.Now().Add(10*time.Minute))).To(BeTrue())
			Expect(didDownload).To(BeTrue())
		})
	})

	Describe("Reload()", func() {
		BeforeEach(func() {
			cache, err = New(10, os.TempDir(), S3Downloader("gondor-north-1"), DownloadTimeout(1*time.Millisecond))
			cache.DownloadFunc = mockDownloader
			f, _ := os.OpenFile(cache.GetFileName(&DownloadRecord{Path: "aragorn"}), os.O_CREATE, 0644)
			f.Close()
			didDownload = false
		})

		It("downloads the file even when we have it", func() {
			cache.Cache.Add("aragorn", cache.GetFileName(&DownloadRecord{Path: "aragorn"}))
			Expect(cache.Reload(&DownloadRecord{Path: "aragorn"})).To(BeTrue())
			Expect(didDownload).To(BeTrue())
		})
	})

	Describe("onEvictDelete()", func() {
		BeforeEach(func() {
			cache, _ = New(10, ".", S3Downloader("gondor-north-1"), DownloadTimeout(1*time.Millisecond))
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

	Describe("GetFileName()", func() {
		BeforeEach(func() {
			cache, _ = New(10, ".", S3Downloader("gondor-north-1"), DownloadTimeout(1*time.Millisecond))
		})

		It("fetches the expected file name for S3 downloads with nil args", func() {
			dr, _ := NewDownloadRecord(s3FilePath, nil)
			fname := cache.GetFileName(dr)

			Expect(fname).To(Equal("4f/a197d51bc70c732281b46e122ff7af17.bar"))
		})

		It("fetches the expected file name for S3 downloads with non-nil args", func() {
			args := map[string]string{
				"DummyHeader": "SomeValue",
			}
			dr, _ := NewDownloadRecord(s3FilePath, args)
			fname := cache.GetFileName(dr)

			Expect(fname).To(Equal("4f/a197d51bc70c732281b46e122ff7af17.bar"))
		})

		It("fetches the expected file name for Dropbox downloads", func() {
			args := map[string]string{
				dropboxAccessToken: "KnockKnock",
				"DummyHeader":      "SomeValue",
			}
			dr, _ := NewDownloadRecord(dropboxFilePath, args)
			fname := cache.GetFileName(dr)

			Expect(fname).To(Equal("8b/5e92c8291b661710e0d1d25db4053f0d_1ff55f50db16da0ad21b8d68ce5aa8cb.bar"))
		})

		It("appends a default extension when there is not one on the original file", func() {
			cache.DefaultExtension = ".foo"
			fname := cache.GetFileName(&DownloadRecord{Path: "missing-an-extension"})

			Expect(fname).To(HaveSuffix(".foo"))
		})

		It("doesn't append the default extension when the original has one", func() {
			cache.DefaultExtension = ".foo"
			fname := cache.GetFileName(&DownloadRecord{Path: "has-an-extension.asdf"})

			Expect(fname).To(HaveSuffix(".asdf"))
		})

		It("prepends a directory to the file path with its name being the first byte of the FNV32 hash of the file name", func() {
			fname1 := cache.GetFileName(&DownloadRecord{Path: "james_joyce.pdf"})
			fname2 := cache.GetFileName(&DownloadRecord{Path: "oscar_wilde.pdf"})

			dir1 := filepath.Dir(fname1)
			dir2 := filepath.Dir(fname2)

			Expect(dir1).To(Equal("d3"))
			Expect(dir2).To(Equal("dc"))
		})

		Context("With DowloadRecord with existing Args", func() {
			It("should include the hashed arguments and extension with _ prefix", func() {
				cache, _ = New(10, "mordor-south-1", DropboxDownloader(), DownloadTimeout(1*time.Millisecond))
				args := map[string]string{
					"Location":  "Mordor",
					"Character": "Gollum",
				}
				fname := cache.GetFileName(&DownloadRecord{Path: "golum-arrived.pub", Args: args})

				Expect(fname).To(HavePrefix("mordor-south-1"))
				Expect(len(strings.Split(fname, "_"))).To(Equal(2))
				Expect(fname).To(ContainSubstring("_"))
				Expect(fname).To(HaveSuffix(".pub"))

			})

			It("should not included hashed arguments and _ when Args is nil", func() {
				cache, _ = New(10, "mordor-south-1", DropboxDownloader(), DownloadTimeout(1*time.Millisecond))
				fname := cache.GetFileName(&DownloadRecord{Path: "golum-arrived.pub", Args: nil})
				Expect(fname).To(HavePrefix("mordor-south-1"))
				Expect(fname).NotTo(ContainSubstring("_"))
				Expect(fname).To(HaveSuffix(".pub"))
			})
		})
	})

	Describe("NewDownloadRecord()", func() {
		dr, err := NewDownloadRecord(s3FilePath, nil)

		It("should not return an error", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("strips leading '/documents'", func() {
			Expect(dr.Path).To(Not(ContainSubstring("/documents")))
		})

		// TODO: Revisit this in the future!
		It("doesn't strip the bucket name from the path", func() {
			Expect(dr.Path).To(ContainSubstring("test-bucket/"))
		})

		It("doesn't return a leading slash", func() {
			Expect(dr.Path).To(Not(HavePrefix("/")))
		})

		It("returns an error if the filename doesn't have enough components", func() {
			dr, err = NewDownloadRecord("/documents/foo-file.pdf", nil)
			Expect(err).Should(HaveOccurred())
		})

		It("uses the dropbox downloader for documents with bucket = 'dropbox'", func() {
			dr, err = NewDownloadRecord(dropboxFilePath, nil)
			Expect(err).Should(Succeed())
			Expect(dr.Manager).Should(BeEquivalentTo(DownloadMangerDropbox))
		})

		It("HashedArgs is empty if no HashableArgs args are passed in", func() {
			Expect(dr.HashedArgs).To(BeEmpty())
		})
	})

	Describe("HashedArgs", func() {
		It("should hash only the HashableArgs", func() {
			args := map[string]string{
				"DropboxAccessToken": "Frodo",
				"FoobarAccessToken":  "Bilbo",
			}
			mockRecord, _ := NewDownloadRecord(dropboxFilePath, args)
			sum := md5.Sum([]byte(args["DropboxAccessToken"]))
			want := fmt.Sprintf("%x", sum[:])

			Expect(mockRecord.HashedArgs).To(Equal(want))
		})

		It("should ignore header name casing", func() {
			args := map[string]string{
				"Dropboxaccesstoken": "Frodo",
			}
			mockRecord, _ := NewDownloadRecord(dropboxFilePath, args)
			sum := md5.Sum([]byte(args["Dropboxaccesstoken"]))
			want := fmt.Sprintf("%x", sum[:])

			Expect(mockRecord.HashedArgs).To(Equal(want))
		})
	})
})
