package metadata_test

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/awslabs/soci-snapshotter/metadata"
	bolt "go.etcd.io/bbolt"
)

// TestBoltVsMemP100 is a test (not benchmark) that prints wall-clock comparison
// for 100 concurrent metadata inits simulating 10 images x 10 layers.
func TestBoltVsMemP100(t *testing.T) {
	const parallelism = 100
	const filesPerLayer = 5000
	toc := buildTOC(filesPerLayer)
	sr := io.NewSectionReader(zeroReaderAt{}, 0, 1<<30)

	// --- Bolt ---
	tmpDir := t.TempDir()
	bOpts := &bolt.Options{
		NoFreelistSync:  true,
		InitialMmapSize: 64 * 1024 * 1024,
		FreelistType:    bolt.FreelistMapType,
	}
	dbPath := fmt.Sprintf("%s/metadata.db", tmpDir)
	db, err := bolt.Open(dbPath, 0600, bOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Warm up bolt
	if _, err := metadata.NewReader(db, sr, toc); err != nil {
		t.Fatal(err)
	}

	// Run bolt
	boltWall, boltMean, boltMax := runParallel(t, parallelism, func() error {
		_, err := metadata.NewReader(db, sr, toc)
		return err
	})

	// --- Memory ---
	// Warm up
	if _, err := metadata.NewMemReader(sr, toc); err != nil {
		t.Fatal(err)
	}

	memWall, memMean, memMax := runParallel(t, parallelism, func() error {
		_, err := metadata.NewMemReader(sr, toc)
		return err
	})

	fmt.Printf("\n=== Bolt vs Memory: %d concurrent layer inits (%d files/layer) ===\n", parallelism, filesPerLayer)
	fmt.Printf("Bolt:   wall=%v  mean=%v  max=%v\n", boltWall, boltMean, boltMax)
	fmt.Printf("Memory: wall=%v  mean=%v  max=%v\n", memWall, memMean, memMax)
	fmt.Printf("Speedup: %.2fx wall-clock\n", float64(boltWall)/float64(memWall))
}

func runParallel(t *testing.T, n int, fn func() error) (wall, mean, max time.Duration) {
	var wg sync.WaitGroup
	durations := make([]time.Duration, n)
	errs := make([]error, n)

	wg.Add(n)
	start := time.Now()
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			s := time.Now()
			errs[idx] = fn()
			durations[idx] = time.Since(s)
		}(i)
	}
	wg.Wait()
	wall = time.Since(start)

	for i, e := range errs {
		if e != nil {
			t.Fatalf("goroutine %d failed: %v", i, e)
		}
	}

	var total time.Duration
	for _, d := range durations {
		total += d
		if d > max {
			max = d
		}
	}
	mean = total / time.Duration(n)
	return
}
