package filecache

import (
	"crypto/md5"
	"errors"
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

const (
	DownloadMangerS3 = iota
	DownloadMangerDropbox
)

var (
	errInvalidURLPath = errors.New("invalid URL path")
	// HashableArgs allows us to support various authentication headers in the future
	HashableArgs = map[string]struct{}{}
)

type DownloadManager int

// DownloadRecord contains information about a file which will be downloaded
type DownloadRecord struct {
	Manager    DownloadManager
	Path       string
	Args       map[string]string
	HashedArgs string
}

type RecordDownloaderFunc = func(dr *DownloadRecord, localFile *os.File) error

// FileCache is a wrapper for hashicorp/golang-lru
type FileCache struct {
	BaseDir          string
	Cache            *lru.Cache
	Waiting          map[string]chan struct{}
	WaitLock         sync.Mutex
	DownloadFunc     func(dr *DownloadRecord, localPath string) error
	OnEvict          func(key interface{}, value interface{})
	DefaultExtension string
	DownloadTimeout  time.Duration
	downloaders      map[DownloadManager]RecordDownloaderFunc
}

type option func(*FileCache) error

func setSize(size int) option {
	return func(c *FileCache) error {
		cache, err := lru.NewWithEvict(size, c.onEvictDelete)
		if err != nil {
			return fmt.Errorf("invalid size: %s", err)
		}

		c.Cache = cache

		return nil
	}
}

func setBaseDir(baseDir string) option {
	return func(c *FileCache) error {
		if baseDir == "" {
			return errors.New("empty baseDir")
		}

		c.BaseDir = baseDir

		return nil
	}
}

// DownloadTimeout sets the file download timeout
func DownloadTimeout(timeout time.Duration) option {
	return func(c *FileCache) error {
		c.DownloadTimeout = timeout

		return nil
	}
}

// DefaultExtension sets the default extension which will be appended to
// cached files in the local directory
func DefaultExtension(ext string) option {
	return func(c *FileCache) error {
		c.DefaultExtension = ext

		return nil
	}
}

// S3Downloader allows the DownloadFunc to pull files from S3 buckets.
// Bucket names are passed at the first part of the path in files requested
// from the cache. Bubbles up errors from the Hashicrorp LRU library
// when something goes wrong there.
func S3Downloader(awsRegion string) option {
	return func(c *FileCache) error {
		c.downloaders[DownloadMangerS3] = func(dr *DownloadRecord, localFile *os.File) error {
			return NewS3RegionManagedDownloader(awsRegion).Download(
				dr, localFile, c.DownloadTimeout,
			)
		}

		return nil
	}
}

// DropboxDownloader allows the DownloadFunc to pull files from Dropbox
// accounts. Bubbles up errors from the Hashicrorp LRU library when
// something goes wrong there.
func DropboxDownloader() option {
	return func(c *FileCache) error {
		c.downloaders[DownloadMangerDropbox] = func(dr *DownloadRecord, localFile *os.File) error {
			return DropboxDownload(dr, localFile, c.DownloadTimeout)
		}

		return nil
	}
}

// download is a generic wrapper which performs common actions before delegating to the
// specific downloader implementations
func (c *FileCache) download(dr *DownloadRecord, localPath string) error {
	directory := filepath.Dir(localPath)
	if directory != "." {
		// Make sure the path to the local file exists
		log.Debugf("MkdirAll() on %s", filepath.Dir(localPath))
		err := os.MkdirAll(filepath.Dir(localPath), 0755)
		if err != nil {
			return fmt.Errorf("could not create local directory: %s", err)
		}
	}

	localFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("could not create local file: %s", err)
	}
	defer localFile.Close()

	if downloader, ok := c.downloaders[dr.Manager]; ok {
		return downloader(dr, localFile)
	}

	return fmt.Errorf("no dowloader found for %q", dr.Path)
}

// New returns a properly configured cache. Bubbles up errors from the Hashicrorp
// LRU library when something goes wrong there. The configured cache will have a
// noop DownloadFunc, which should be replaced if you want to actually get files
// from somewhere. Or, look at NewS3Cache() which is backed by Amazon S3.
func New(size int, baseDir string, opts ...option) (*FileCache, error) {
	fCache := &FileCache{
		Waiting:     make(map[string]chan struct{}),
		downloaders: make(map[DownloadManager]RecordDownloaderFunc),
	}
	fCache.DownloadFunc = fCache.download

	if err := setSize(size)(fCache); err != nil {
		return nil, err
	}

	if err := setBaseDir(baseDir)(fCache); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		err := opt(fCache)
		if err != nil {
			return nil, fmt.Errorf("invalid option: %s", err)
		}
	}

	return fCache, nil
}

// FetchNewerThan will look in the cache for a file, make sure it's newer than
// timestamp, and if so return true. Otherwise it will possibly download the file
// and only return false if it's unable to do so.
func (c *FileCache) FetchNewerThan(dr *DownloadRecord, timestamp time.Time) bool {
	if !c.Contains(dr) {
		return c.Fetch(dr)
	}

	storagePath := c.GetFileName(dr)
	stat, err := times.Stat(storagePath)
	if err != nil {
		return c.Fetch(dr)
	}

	// We use mtime because the file could have been overwritten with new data
	// Compare the timestamp, and need to check the cache again... could have changed
	if c.Contains(dr) && timestamp.Before(stat.ModTime()) {
		return true
	}

	return c.Reload(dr)
}

// Fetch will return true if we have the file, or will go download the file and
// return true if we can. It will return false only if it's unable to fetch the
// file from the backing store (S3).
func (c *FileCache) Fetch(dr *DownloadRecord) bool {
	if c.Contains(dr) {
		return true
	}

	err := c.MaybeDownload(dr)
	if err != nil {
		log.Errorf("Tried to fetch file %s, got '%s'", dr.Path, err)
		return false
	}

	return true
}

// Reload will remove a file from the cache and attempt to reload from the
// backing store, calling MaybeDownload().
func (c *FileCache) Reload(dr *DownloadRecord) bool {
	c.Cache.Remove(dr.GetUniqueName())

	err := c.MaybeDownload(dr)
	if err != nil {
		log.Errorf("Tried to fetch file %s, got '%s'", dr.Path, err)
		return false
	}

	return true
}

// Contains looks to see if we have an entry in the cache for this file.
func (c *FileCache) Contains(dr *DownloadRecord) bool {
	return c.Cache.Contains(dr.GetUniqueName())
}

// MaybeDownload might go out to the backing store (S3) and get the file if the
// file isn't already being downloaded in another routine. In both cases it will
// block until the download is completed either by this goroutine or another one.
func (c *FileCache) MaybeDownload(dr *DownloadRecord) error {
	// See if someone is already downloading
	c.WaitLock.Lock()
	if waitChan, ok := c.Waiting[dr.GetUniqueName()]; ok {
		c.WaitLock.Unlock()

		log.Debugf("Awaiting download of %s", dr.Path)
		<-waitChan
		return nil
	}

	// The file could have arrived while we were getting here
	if c.Contains(dr) {
		c.WaitLock.Unlock()
		return nil
	}

	// Still don't have it, let's fetch it.
	// This tells other goroutines that we're fetching, and
	// lets us signal completion.
	log.Debugf("Making channel for %s", dr.Path)
	c.Waiting[dr.GetUniqueName()] = make(chan struct{})
	c.WaitLock.Unlock()

	// Ensure we don't leave the channel open when leaving this function
	defer func() {
		c.WaitLock.Lock()
		log.Debugf("Deleting channel for %s", dr.Path)
		close(c.Waiting[dr.GetUniqueName()])  // Notify anyone waiting on us
		delete(c.Waiting, dr.GetUniqueName()) // Remove it from the waiting map
		c.WaitLock.Unlock()
	}()

	storagePath := c.GetFileName(dr)
	err := c.DownloadFunc(dr, storagePath)
	if err != nil {
		return err
	}

	c.Cache.Add(dr.GetUniqueName(), storagePath)

	return nil
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

// GetFileName returns the full storage path and file name for a file, if it were
// in the cache. This does _not_ check to see if the file is actually _in_ the
// cache. This builds a cache structure of up to 256 directories, each beginning
// with the first 2 letters of the FNV32 hash of the filename. This is then joined
// to the base dir and MD5 hashed filename to form the cache path for each file.
// It preserves the file extension (if present)
//
// e.g. /base_dir/2b/b0804ec967f48520697662a204f5fe72
//
func (c *FileCache) GetFileName(dr *DownloadRecord) string {
	hashedFilename := md5.Sum([]byte(dr.Path))
	fnvHasher := fnv.New32()
	// The current implementation of fnv.New32().Write never returns a non-nil error
	_, err := fnvHasher.Write([]byte(dr.Path))
	if err != nil {
		log.Errorf("Failed to compute the fnv hash: %s", err)
	}
	hashedDir := fnvHasher.Sum(nil)

	// If we don't find an original file extension, we'll default to this one
	extension := c.DefaultExtension

	// Look in the last 5 characters for a . and extension
	lastDot := strings.LastIndexByte(dr.Path, '.')
	if lastDot > len(dr.Path)-6 {
		extension = dr.Path[lastDot:]
	}

	var fileName string
	if len(dr.Args) != 0 {
		// in order to avoid file cache collision on the same filename, if we
		// have existing HTTP headers into the dr.Args append their
		// hashed value between the hashedFilename and extension with _ prefix
		fileName = fmt.Sprintf("%x_%s%s", hashedFilename, dr.HashedArgs, extension)
	} else {
		fileName = fmt.Sprintf("%x%s", hashedFilename, extension)
	}

	dir := fmt.Sprintf("%x", hashedDir[:1])
	return filepath.Join(c.BaseDir, dir, filepath.FromSlash(path.Clean("/"+fileName)))
}

// getHashedArgs computes the MD5 sum of the arguments existing in a DownloadRecord
// matching HashableArgs array and return the hashed value as a hex-encoded string
func getHashedArgs(args map[string]string) string {
	if len(args) == 0 {
		return ""
	}

	var builder strings.Builder
	for hashableArg := range HashableArgs {
		if arg, ok := args[hashableArg]; ok {
			_, err := builder.WriteString(arg)
			if err != nil {
				continue
			}
		}
	}

	if builder.Len() == 0 {
		return ""
	}

	hashedArgs := md5.Sum([]byte(builder.String()))

	return fmt.Sprintf("%x", string(hashedArgs[:]))
}

// bucketToDownloadManager matches the given bucket to a suitable download manager
// TODO: Implement this in a more robust / generic way
func bucketToDownloadManager(bucket string) DownloadManager {
	switch bucket {
	case "dropbox":
		return DownloadMangerDropbox
	default:
		return DownloadMangerS3
	}
}

// NewDownloadRecord converts the incoming URL path into a download record containing a cached
// filename (this is the filename on the backing store, not the cached filename locally)
// together with the args needed for authentication
func NewDownloadRecord(url string, args map[string]string) (*DownloadRecord, error) {
	pathParts := strings.Split(strings.TrimPrefix(url, "/documents/"), "/")

	// We need at least a bucket and filename
	if len(pathParts) < 2 {
		return nil, errInvalidURLPath
	}

	path := strings.Join(pathParts, "/")

	if path == "" || path == "/" {
		return nil, errInvalidURLPath
	}

	// Make sure all arg names are lower case and contain only the ones we recognise
	normalisedArgs := make(map[string]string, len(args))
	for arg, value := range args {
		normalisedArg := strings.ToLower(arg)
		if _, ok := HashableArgs[normalisedArg]; !ok {
			continue
		}
		normalisedArgs[normalisedArg] = value
	}

	return &DownloadRecord{
		Manager:    bucketToDownloadManager(pathParts[0]),
		Path:       path,
		Args:       normalisedArgs,
		HashedArgs: getHashedArgs(normalisedArgs),
	}, nil
}

// GetUniqueName returns a *HOPEFULLY* unique name for the download record
func (dr *DownloadRecord) GetUniqueName() string {
	if len(dr.Args) > 0 {
		return fmt.Sprintf("%s_%s", dr.Path, dr.HashedArgs)
	}

	return dr.Path
}
