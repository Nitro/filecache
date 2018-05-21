package filecache_test

import (
	"context"
	"time"

	. "github.com/Nitro/filecache"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("S3", func() {
	var (
		manager *S3RegionManagedDownloader
	)

	BeforeEach(func() {
		// Reset between runs
		manager = NewS3RegionManagedDownloader("us-west-2")
	})

	Describe("NewS3RegionManagedDownloader()", func() {
		It("returns a properly configured instance", func() {
			Expect(manager).NotTo(BeNil())
			Expect(manager.DefaultRegion).To(Equal("us-west-2"))
			Expect(manager.DownloaderCache).NotTo(BeNil())
		})
	})

	// This test will actually contact S3... not in love with that
	// but don't want to mock it out, either. Could be mocked out:
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3iface/#S3API
	Describe("GetDownloader()", func() {
		It("returns a newly created downloader", func() {
			dLoader, err := manager.GetDownloader(context.Background(), "nitro-public")

			Expect(err).To(BeNil())
			Expect(dLoader).NotTo(BeNil())
			Expect(dLoader.S3).NotTo(BeNil())
		})

		It("returns a cached downloader", func() {
			dLoader1, err := manager.GetDownloader(context.Background(), "nitro-public")
			Expect(err).To(BeNil())

			dLoader2, err := manager.GetDownloader(context.Background(), "nitro-public")
			Expect(err).To(BeNil())

			Expect(dLoader1).To(Equal(dLoader2))
		})

		It("returns an error when trying to fetch a file which doesn't exist", func() {
			err := manager.Download("non-existent-bucket/foo.pdf", "foo.pdf", 1*time.Second)
			Expect(err.Error()).To(ContainSubstring("Could not fetch from S3"))
		})

		It("returns an error when getting a 0 length file", func() {
			err := manager.Download("nitro-junk/foo.pdf", "foo.pdf", 1*time.Second)
			Expect(err.Error()).To(ContainSubstring("0 length file received from S3"))
		})
	})
})
