package filecache

import (
	"crypto/md5"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
)

// FileCache is a wrapper for hashicorp/golang-lru
type FileCache struct {
	BaseDir      string
	Cache        *lru.Cache
	Waiting      map[string]chan struct{}
	WaitLock     sync.Mutex
	DownloadFunc func(fname string, url string, localPath string) error
	OnEvict      func(key interface{}, value interface{})
}

// New returns a properly configured cache. Bubbles up errors from the Hashicrorp
// LRU library when something goes wrong there. The configured cache will have a
// noop DownloadFunc, which should be replaced if you want to actually get files
// from somewhere. Or, look at NewS3Cache() which is backed by Amazon S3.
func New(size int, baseDir string) (*FileCache, error) {
	fCache := &FileCache{
		BaseDir:      baseDir,
		Waiting:      make(map[string]chan struct{}),
		DownloadFunc: func(fname string, url string, localPath string) error { return nil },
	}

	cache, err := lru.NewWithEvict(size, fCache.onEvictDelete)
	if err != nil {
		return nil, err
	}

	fCache.Cache = cache

	return fCache, nil
}

// NewS3Cache returns a cache where the DownloadFunc will pull files from a
// specified S3 bucket. Bubbles up errors from the Hashicrorp LRU library when
// something goes wrong there.
func NewS3Cache(size int, baseDir string, s3Bucket string, awsRegion string) (*FileCache, error) {
	fCache, err := New(size, baseDir)
	if err != nil {
		return nil, err
	}

	fCache.DownloadFunc = func(fname string, url string, localPath string) error {
		return S3Download(fname, localPath, s3Bucket, awsRegion)
	}

	return fCache, nil
}

// Fetch will return true if we have the file, or will go download the file and
// return true if we can. It will return false only if it's unable to fetch the
// file from the backing store (S3). The filename is what we want it to be called
// locally and the URL is the actual request passed to us.
func (c *FileCache) Fetch(filename string, url string) bool {
	// Try a few non-locking on our side. The LRU cache itself handles locking so
	// we can treat it atomically itself.
	if c.Contains(filename) {
		return true
	}

	err := c.MaybeDownload(filename, url)
	if err != nil {
		log.Errorf("Tried to fetch file %s, got '%s'", filename, err)
		return false
	}

	return true
}

// Contains looks to see if we have an entry in the cache for this filename.
func (c *FileCache) Contains(filename string) bool {
	return c.Cache.Contains(filename)
}

// MaybeDownload might go out to the backing store (S3) and get the file if the
// file isn't already being downloaded in another routine. In both cases it will
// block until the download is completed either by this goroutine or another one.
func (c *FileCache) MaybeDownload(filename string, url string) error {
	// See if someone is already downloading
	c.WaitLock.Lock()
	if waitChan, ok := c.Waiting[filename]; ok {
		c.WaitLock.Unlock()

		log.Debugf("Awaiting download of %s", filename)
		<-waitChan
		return nil
	}

	// The file could have arrived while we were getting here
	if c.Contains(filename) {
		c.WaitLock.Unlock()
		return nil
	}

	// Still don't have it, let's fetch it.
	// This tells other goroutines that we're fetching, and
	// lets us signal completion.
	log.Debugf("Making channel for %s", filename)
	c.Waiting[filename] = make(chan struct{})
	c.WaitLock.Unlock()

	// Ensure we don't leave the channel open when leaving this function
	defer func() {
		c.WaitLock.Lock()
		log.Debugf("Deleting channel for %s", filename)
		close(c.Waiting[filename])  // Notify anyone waiting on us
		delete(c.Waiting, filename) // Remove it from the waiting map
		c.WaitLock.Unlock()
	}()

	storagePath := c.GetFileName(filename)
	err := c.DownloadFunc(filename, url, storagePath)
	if err != nil {
		return err
	}

	c.Cache.Add(filename, storagePath)

	return nil
}

// GetFileName returns the full storage path and file name for a file, if it were
// in the cache. This does _not_ check to see if the file is actually _in_ the
// cache. This builds a cache structure of up to 16 directories, each beginning
// with the first letter of the MD5 hash of the filename. This is then joined
// to the base dir and hashed filename to form the cache path for each file.
//
// e.g. /base_dir/b/b0804ec967f48520697662a204f5fe72
//
func (c *FileCache) GetFileName(filename string) string {
	hashed := md5.Sum([]byte(filename))

	file := fmt.Sprintf("%x", hashed)
	dir := fmt.Sprintf("%x", hashed[0])
	return filepath.Join(c.BaseDir, dir, filepath.FromSlash(path.Clean("/"+file)))
}

// onEvictDelete is a callback that is triggered when the LRU cache expires an
// entry.
func (c *FileCache) onEvictDelete(key interface{}, value interface{}) {
	filename := key.(string)
	storagePath := value.(string)

	if c.OnEvict != nil {
		c.OnEvict(key, value)
	}

	log.Debugf("Got eviction notice for '%s', removing", key)

	err := os.Remove(storagePath)
	if err != nil {
		log.Errorf("Unable to evict '%s' at local path '%s': %s", filename, storagePath, err)
		return
	}
}
