// Copyright (C) 2026 Pau Nadeu.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package genfence keeps the tombstones of fenced generations.
//
// A tombstone says that a generation of a tenant is fenced: the server
// refuses every later command for it. Tombstones are never removed. They are
// kept in a bbolt file, so a lookup is a B+tree read and the set does not have
// to fit in memory, and every fence is synced to disk before it is
// acknowledged. A bounded LRU cache in front of the file answers repeated
// lookups, fenced or not, without reading it.
//
// File layout, format 1 (FileName under the configured directory):
//
//	bucket "meta":        "format" -> 0x01
//	bucket "tombstones":  key   = uint16 BE len(tenant) | tenant | uint64 BE generation
//	                      value = 0x01 | int64 BE fenced-at Unix nanoseconds (UTC)
//	                              | uint16 BE len(speaker) | speaker
//
// Keys sort by tenant, then by generation.
package genfence

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// FileName is the name of the store file in its directory.
const FileName = "generation-fence.db"

const (
	formatVersion = 1
	recordVersion = 1
	// MaxField is the longest tenant or speaker, in bytes.
	MaxField = 256
)

var (
	bucketMeta       = []byte("meta")
	bucketTombstones = []byte("tombstones")
	keyFormat        = []byte("format")
)

// DefaultCacheSize is the number of lookups the cache keeps when the caller
// does not say.
const DefaultCacheSize = 65536

// OpenTimeout bounds the wait for the file lock another process holds. A
// variable so that tests can shorten it.
var OpenTimeout = 5 * time.Second

// Tombstone is a fenced generation.
type Tombstone struct {
	FencedAt time.Time
	Speaker  string
}

type key struct {
	tenant     string
	generation uint64
}

type cacheEntry struct {
	key       key
	fenced    bool
	tombstone Tombstone
}

// Store is safe for concurrent use.
type Store struct {
	mu    sync.Mutex
	db    *bolt.DB
	cap   int
	lru   *list.List
	items map[key]*list.Element
}

// Open opens or creates the store in dir. It fails when another process
// holds the file for longer than OpenTimeout.
func Open(dir string, cacheSize int) (*Store, error) {
	if dir == "" {
		return nil, errors.New("generation fence directory is not set")
	}
	if cacheSize <= 0 {
		cacheSize = DefaultCacheSize
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create generation fence directory: %w", err)
	}
	path := filepath.Join(dir, FileName)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: OpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("open generation fence store %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if v := meta.Get(keyFormat); v != nil {
			if len(v) != 1 || v[0] != formatVersion {
				return fmt.Errorf("unknown generation fence store format %v", v)
			}
		} else if err := meta.Put(keyFormat, []byte{formatVersion}); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketTombstones)
		return err
	})
	if err == nil && created {
		// The commit synced the file; the directory entry needs its own sync.
		err = syncDir(dir)
	}
	if err != nil {
		//nolint: errcheck // the initialization error is the one to report
		db.Close()
		return nil, fmt.Errorf("initialize generation fence store %s: %w", path, err)
	}
	return &Store{
		db:    db,
		cap:   cacheSize,
		lru:   list.New(),
		items: make(map[key]*list.Element, cacheSize),
	}, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close closes the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func encodeKey(tenant string, generation uint64) []byte {
	b := make([]byte, 2+len(tenant)+8)
	binary.BigEndian.PutUint16(b, uint16(len(tenant)))
	copy(b[2:], tenant)
	binary.BigEndian.PutUint64(b[2+len(tenant):], generation)
	return b
}

func encodeValue(t Tombstone) []byte {
	b := make([]byte, 1+8+2+len(t.Speaker))
	b[0] = recordVersion
	binary.BigEndian.PutUint64(b[1:], uint64(t.FencedAt.UnixNano()))
	binary.BigEndian.PutUint16(b[9:], uint16(len(t.Speaker)))
	copy(b[11:], t.Speaker)
	return b
}

func decodeValue(b []byte) (Tombstone, error) {
	if len(b) < 11 || b[0] != recordVersion {
		return Tombstone{}, fmt.Errorf("bad tombstone record of %d bytes", len(b))
	}
	n := int(binary.BigEndian.Uint16(b[9:]))
	if len(b) != 11+n {
		return Tombstone{}, fmt.Errorf("bad tombstone record of %d bytes", len(b))
	}
	return Tombstone{
		FencedAt: time.Unix(0, int64(binary.BigEndian.Uint64(b[1:]))).UTC(),
		Speaker:  string(b[11:]),
	}, nil
}

func validate(tenant string, generation uint64, speaker string) error {
	if tenant == "" || len(tenant) > MaxField {
		return fmt.Errorf("tenant must be 1 to %d bytes", MaxField)
	}
	if generation == 0 {
		return errors.New("generation must not be 0")
	}
	if len(speaker) > MaxField {
		return fmt.Errorf("speaker must be at most %d bytes", MaxField)
	}
	return nil
}

func (s *Store) cached(k key) (*cacheEntry, bool) {
	e, ok := s.items[k]
	if !ok {
		return nil, false
	}
	s.lru.MoveToFront(e)
	return e.Value.(*cacheEntry), true
}

func (s *Store) remember(c *cacheEntry) {
	if e, ok := s.items[c.key]; ok {
		e.Value = c
		s.lru.MoveToFront(e)
		return
	}
	s.items[c.key] = s.lru.PushFront(c)
	for s.lru.Len() > s.cap {
		last := s.lru.Back()
		s.lru.Remove(last)
		delete(s.items, last.Value.(*cacheEntry).key)
	}
}

// Lookup reports whether the generation is fenced. An error means the answer
// is not known; the caller must treat the generation as fenced.
func (s *Store) Lookup(tenant string, generation uint64) (Tombstone, bool, error) {
	if err := validate(tenant, generation, ""); err != nil {
		return Tombstone{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return Tombstone{}, false, errors.New("generation fence store is closed")
	}
	k := key{tenant, generation}
	if c, ok := s.cached(k); ok {
		return c.tombstone, c.fenced, nil
	}
	var t Tombstone
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketTombstones).Get(encodeKey(tenant, generation))
		if v == nil {
			return nil
		}
		var err error
		t, err = decodeValue(v)
		found = err == nil
		return err
	})
	if err != nil {
		return Tombstone{}, false, fmt.Errorf("read tombstone: %w", err)
	}
	s.remember(&cacheEntry{key: k, fenced: found, tombstone: t})
	return t, found, nil
}

// Fence writes the tombstone and syncs it before it returns. It is
// idempotent: fencing a fenced generation returns the first tombstone and
// newly false.
func (s *Store) Fence(tenant string, generation uint64, speaker string, now time.Time) (Tombstone, bool, error) {
	if err := validate(tenant, generation, speaker); err != nil {
		return Tombstone{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return Tombstone{}, false, errors.New("generation fence store is closed")
	}
	k := key{tenant, generation}
	if c, ok := s.cached(k); ok && c.fenced {
		return c.tombstone, false, nil
	}
	t := Tombstone{FencedAt: now.UTC(), Speaker: speaker}
	newly := true
	// bbolt syncs the file before Update returns.
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTombstones)
		kb := encodeKey(tenant, generation)
		if v := b.Get(kb); v != nil {
			old, err := decodeValue(v)
			if err != nil {
				return err
			}
			t, newly = old, false
			return nil
		}
		return b.Put(kb, encodeValue(t))
	})
	if err != nil {
		return Tombstone{}, false, fmt.Errorf("write tombstone: %w", err)
	}
	s.remember(&cacheEntry{key: k, fenced: true, tombstone: t})
	return t, newly, nil
}

// FenceMany fences several generations of a tenant in one transaction,
// synced once before it returns: either every tombstone is written or none
// is. It is idempotent per generation, like Fence. The generations must be
// distinct; the results are in their order.
func (s *Store) FenceMany(tenant string, generations []uint64, speaker string, now time.Time) ([]Tombstone, []bool, error) {
	seen := make(map[uint64]struct{}, len(generations))
	for _, g := range generations {
		if err := validate(tenant, g, speaker); err != nil {
			return nil, nil, err
		}
		if _, dup := seen[g]; dup {
			return nil, nil, fmt.Errorf("generation %d is listed twice", g)
		}
		seen[g] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil, nil, errors.New("generation fence store is closed")
	}
	ts := make([]Tombstone, len(generations))
	newly := make([]bool, len(generations))
	fresh := Tombstone{FencedAt: now.UTC(), Speaker: speaker}
	// bbolt syncs the file before Update returns.
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTombstones)
		for i, g := range generations {
			kb := encodeKey(tenant, g)
			if v := b.Get(kb); v != nil {
				old, err := decodeValue(v)
				if err != nil {
					return err
				}
				ts[i] = old
				continue
			}
			if err := b.Put(kb, encodeValue(fresh)); err != nil {
				return err
			}
			ts[i], newly[i] = fresh, true
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("write tombstones: %w", err)
	}
	for i, g := range generations {
		s.remember(&cacheEntry{key: key{tenant, g}, fenced: true, tombstone: ts[i]})
	}
	return ts, newly, nil
}

// CacheLen is the number of cached lookups.
func (s *Store) CacheLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}

// Count is the number of tombstones in the file.
func (s *Store) Count() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return 0, errors.New("generation fence store is closed")
	}
	n := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketTombstones).Stats().KeyN
		return nil
	})
	return n, err
}
