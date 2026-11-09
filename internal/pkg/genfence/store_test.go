package genfence

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestFenceIsIdempotentAndDurable(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 4)
	require.NoError(t, err)

	_, fenced, err := s.Lookup("t1", 7)
	require.NoError(t, err)
	require.False(t, fenced)

	at := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	first, newly, err := s.Fence("t1", 7, "instance-a", at)
	require.NoError(t, err)
	require.True(t, newly)
	require.Equal(t, at, first.FencedAt)

	again, newly, err := s.Fence("t1", 7, "instance-b", at.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, newly)
	require.Equal(t, first, again, "a second fence keeps the first tombstone")

	got, fenced, err := s.Lookup("t1", 7)
	require.NoError(t, err)
	require.True(t, fenced)
	require.Equal(t, first, got)
	require.NoError(t, s.Close())

	s, err = Open(dir, 4)
	require.NoError(t, err)
	defer s.Close()
	got, fenced, err = s.Lookup("t1", 7)
	require.NoError(t, err)
	require.True(t, fenced, "the tombstone survives a reopen")
	require.Equal(t, first, got)
	_, fenced, err = s.Lookup("t1", 8)
	require.NoError(t, err)
	require.False(t, fenced, "the fence is per generation")
	_, fenced, err = s.Lookup("t2", 7)
	require.NoError(t, err)
	require.False(t, fenced, "the fence is per namespace")
}

// A namespace that is a prefix of another does not collide with it.
func TestTombstoneKeysDoNotCollide(t *testing.T) {
	require.NotEqual(t, encodeKey("ab", 1), encodeKey("a", 0x6200000000000000|1))
	s, err := Open(t.TempDir(), 0)
	require.NoError(t, err)
	defer s.Close()
	_, _, err = s.Fence("a", 1, "", time.Now())
	require.NoError(t, err)
	_, fenced, err := s.Lookup("ab", 1)
	require.NoError(t, err)
	require.False(t, fenced)
}

// The cache stays within its bound, and a lookup that misses it reads the
// file, so the set of tombstones does not have to fit in memory.
func TestCacheIsBounded(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 3)
	require.NoError(t, err)
	defer s.Close()
	for g := uint64(1); g <= 50; g++ {
		_, _, err := s.Fence("t", g, "", time.Now())
		require.NoError(t, err)
		require.LessOrEqual(t, s.CacheLen(), 3)
	}
	for g := uint64(1); g <= 60; g++ {
		_, fenced, err := s.Lookup("t", g)
		require.NoError(t, err)
		require.Equal(t, g <= 50, fenced, "generation %d", g)
		require.LessOrEqual(t, s.CacheLen(), 3)
	}
	n, err := s.Count()
	require.NoError(t, err)
	require.Equal(t, 50, n)
}

func TestInvalidArguments(t *testing.T) {
	s, err := Open(t.TempDir(), 0)
	require.NoError(t, err)
	defer s.Close()
	_, _, err = s.Fence(string(make([]byte, MaxField+1)), 1, "", time.Now())
	require.Error(t, err)
	_, _, err = s.Fence("", 1, "", time.Now())
	require.NoError(t, err, "the namespace may be empty")
	_, _, err = s.Fence("t", 0, "", time.Now())
	require.Error(t, err)
	_, _, err = s.Lookup(string(make([]byte, MaxField+1)), 1)
	require.Error(t, err)
	_, err = Open("", 0)
	require.Error(t, err)
}

// A second open of the file waits for the lock and then fails, so two
// servers never share one store.
func TestSecondOpenFails(t *testing.T) {
	old := OpenTimeout
	OpenTimeout = 100 * time.Millisecond
	defer func() { OpenTimeout = old }()
	dir := t.TempDir()
	s, err := Open(dir, 0)
	require.NoError(t, err)
	defer s.Close()
	_, err = Open(dir, 0)
	require.Error(t, err)
}

// The on-disk record is the documented format.
func TestRecordFormat(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 0)
	require.NoError(t, err)
	at := time.Unix(0, 1_700_000_000_123_456_789).UTC()
	_, _, err = s.Fence("ten", 0x0102030405060708, "sp", at)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := bolt.Open(filepath.Join(dir, FileName), 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		require.Equal(t, []byte{1}, tx.Bucket([]byte("meta")).Get([]byte("format")))
		v := tx.Bucket([]byte("tombstones")).Get([]byte{0, 3, 't', 'e', 'n', 1, 2, 3, 4, 5, 6, 7, 8})
		require.NotNil(t, v)
		want := []byte{1}
		want = append(want, 0x17, 0x97, 0x9c, 0xfe, 0x3d, 0x85, 0xcd, 0x15)
		want = append(want, 0, 2, 's', 'p')
		require.Equal(t, want, v)
		return nil
	}))
}

func TestUnknownFormatIsRefused(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 0)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	db, err := bolt.Open(filepath.Join(dir, FileName), 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("format"), []byte{9})
	}))
	require.NoError(t, db.Close())
	_, err = Open(dir, 0)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(dir, FileName))
	require.NoError(t, err)
}
