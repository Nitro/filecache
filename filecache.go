package filecache

import (
	"crypto/md5"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/djherbis/times"
	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
)

// FileCache is a wrapper for hashicorp/golang-lru
type FileCache struct {
	BaseDir      string
	Cache        *lru.Cache
	Waiting      map[string]chan struct{}
	WaitLock     sync.Mutex
	DownloadFunc func(fname string, localPath string) error
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
		DownloadFunc: func(fname string, localPath string) error { return nil },
	}

	cache, err := lru.NewWithEvict(size, fCache.onEvictDelete)
	if err != nil {
		return nil, err
	}

	fCache.Cache = cache

	return fCache, nil
}

// NewS3Cache returns a cache where the DownloadFunc will pull files from
// S3 buckets. Bucket names are passed at the first part of the path in
// files requested from the cache. Bubbles up errors from the Hashicrorp LRU
// library when something goes wrong there.
func NewS3Cache(size int, baseDir string, awsRegion string,
	downloadTimeout time.Duration) (*FileCache, error) {

	fCache, err := New(size, baseDir)
	if err != nil {
		return nil, err
	}

	manager := NewS3RegionManagedDownloader(awsRegion)

	fCache.DownloadFunc = func(fname string, localPath string) error {
		return manager.Download(fname, localPath, downloadTimeout)
	}

	return fCache, nil
}

// FetchNewerThan will look in the cache for a file, make sure it's newer than
// timestamp, and if so return true. Otherwise it will possibly download the file
// and only return false if it's unable to do so.
func (c *FileCache) FetchNewerThan(filename string, timestamp time.Time) bool {
	if !c.Contains(filename) {
		return c.Fetch(filename)
	}

	storagePath := c.GetFileName(filename)
	stat, err := times.Stat(storagePath)
	if err != nil {
		return c.Fetch(filename)
	}

	// We use mtime because the file could have been overwritten with new data
	// Compare the timestamp, and need to check the cache again... could have changed
	if c.Contains(filename) && timestamp.Before(stat.ModTime()) {
		return true
	}

	return c.Reload(filename)
}

// Fetch will return true if we have the file, or will go download the file and
// return true if we can. It will return false only if it's unable to fetch the
// file from the backing store (S3).
func (c *FileCache) Fetch(filename string) bool {
	if c.Contains(filename) {
		return true
	}

	err := c.MaybeDownload(filename)
	if err != nil {
		log.Errorf("Tried to fetch file %s, got '%s'", filename, err)
		return false
	}

	return true
}

// Reload will remove a file from the cache and attempt to reload from the
// backing store, calling MaybeDownload().
func (c *FileCache) Reload(filename string) bool {
	c.Cache.Remove(filename)

	err := c.MaybeDownload(filename)
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
func (c *FileCache) MaybeDownload(filename string) error {
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
	err := c.DownloadFunc(filename, storagePath)
	if err != nil {
		return err
	}

	c.Cache.Add(filename, storagePath)

	return nil
}

// GetFileName returns the full storage path and file name for a file, if it were
// in the cache. This does _not_ check to see if the file is actually _in_ the
// cache. This builds a cache structure of up to 256 directories, each beginning
// with the first 2 letters of the FNV32 hash of the filename. This is then joined
// to the base dir and MD5 hashed filename to form the cache path for each file.
// It preserves the file extension (if present)
//
// e.g. /base_dir/2b/b0804ec967f48520697662a204f5fe72
//
func (c *FileCache) GetFileName(filename string) string {
	hashedFilename := md5.Sum([]byte(filename))
	hashedDir := fnv.New32().Sum([]byte(filename))

	var extension string
	// Look in the last 5 characters for a . and extension
	lastDot := strings.LastIndexByte(filename, '.')
	if lastDot > len(filename)-5 {
		extension = filename[lastDot:len(filename)]
	}

	file := fmt.Sprintf("%x%s", hashedFilename, extension)
	dir := fmt.Sprintf("%x", hashedDir[:1])
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

// Purge clears all the files from the cache (via the onEvict callback for each key).
func (c *FileCache) Purge() {
	c.Cache.Purge()
}

// PurgeAsync clears all the files from the cache and takes an optional channel
// to close when the purge has completed.
func (c *FileCache) PurgeAsync(doneChan chan struct{}) {
	go func() {
		c.Purge()
		if doneChan != nil {
			close(doneChan)
		}
	}()
}
