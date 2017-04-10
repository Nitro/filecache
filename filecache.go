package filecache

import (
	"os"
	"path"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/hashicorp/golang-lru"
)

type FileCache struct {
	BaseDir   string
	S3Bucket  string
	AwsRegion string
	Cache     *lru.Cache
	Waiting   map[string]chan struct{}
	waitLock  sync.Mutex
	DownloadFunc func(fname string, localPath string) error
}

// I don't like New() methods that return errors, but that's what
// the Hashicorp lib does. So we kinda have to pass that on or
// the program loses control of the cache creation.
func New(size int, baseDir string, s3Bucket string, awsRegion string) (*FileCache, error) {
	cache, err := lru.NewWithEvict(size, onEvictDelete)
	if err != nil {
		return nil, err
	}

	fCache := &FileCache{
		Cache:   cache,
		BaseDir: baseDir,
		S3Bucket: s3Bucket,
		AwsRegion: awsRegion,
		Waiting: make(map[string]chan struct{}),
	}
	fCache.DownloadFunc = fCache.S3Download

	return fCache, nil
}

// Fetch will return true if we have the file, or will go download
// the file and return true if we can. It will return false only
// if it's unable to fetch the file from the backing store (S3).
func (c *FileCache) Fetch(filename string) bool {
	if c.Contains(filename) {
		return true
	}

	// If we're already fetching it, just wait for it
	if c.maybeAwaitDownload(filename) {
		return true
	}

	err := c.Download(filename)
	if err != nil {
		log.Errorf("Tried to fetch file %s, got '%s'", filename, err)
		return false
	}

	return true
}

// Contains looks to see if we have an entry in the cache for this
// filename.
func (c *FileCache) Contains(filename string) bool {
	return c.Cache.Contains(filename)
}

// Go out to the backing store (S3) and get the file
func (c *FileCache) Download(filename string) error {
	c.waitLock.Lock()
	if c.Contains(filename) {
		c.waitLock.Unlock()
		return nil
	}
	c.Waiting[filename] = make(chan struct{})
	c.waitLock.Unlock()

	// Still don't have it, let's fetch it
	storagePath := c.GetFileName(filename)
	err := c.DownloadFunc(filename, storagePath)
	if err != nil {
		return err
	}

	c.Cache.Add(filename, storagePath)
	close(c.Waiting[filename]) // Notify anyone waiting on us

	c.waitLock.Lock()
	delete(c.Waiting, filename) // Remove it from the waiting map
	c.waitLock.Unlock()

	return nil
}

func (c *FileCache) maybeAwaitDownload(filename string) bool {
	c.waitLock.Lock()
	waitChan, ok := c.Waiting[filename]
	if !ok { // Nobody is downloading it
		c.waitLock.Unlock()
		return false // We didn't wait
	}
	c.waitLock.Unlock()

	log.Debugf("Awaiting download of %s", filename)

	// It was being downloaded, let's wait for that to finish
	<-waitChan

	// And if something there went wrong, we could _still_ not have it.
	// Returning false here could cause Fetch() to retry this
	// once per request currently waiting on the download.
	return c.Cache.Contains(filename)
}

// GetFileName returns the full storage path and file name for a
// file, if it were in the cache. This does _not_ check to see if
// the file is actually _in_ the cache.
func (c *FileCache) GetFileName(filename string) string {
	dir, file := filepath.Split(filename)
	return filepath.Join(c.BaseDir, dir, filepath.FromSlash(path.Clean("/"+file)))
}

// onEvicteDelete is a callback that is triggered when the LRU
// cache expires an entry.
func onEvictDelete(key interface{}, value interface{}) {
	filename := key.(string)
	storagePath := value.(string)

	log.Debugf("Got eviction notice for '%s', removing", key)

	err := os.Remove(storagePath)
	if err != nil {
		log.Errorf("Unable to evict '%s' at local path '%s': %s", filename, storagePath, err)
		return
	}
}
