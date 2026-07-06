/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package metadata_test

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/awslabs/soci-snapshotter/metadata"
	"os"
	bolt "go.etcd.io/bbolt"
)

// benchmarkBoltVsMem compares bolt and in-memory metadata store init at a given parallelism.
func benchmarkBoltVsMem(b *testing.B, parallelism int) {
	toc := buildTOC(5000)
	sr := io.NewSectionReader(zeroReaderAt{}, 0, 1<<30)

	b.Run("Bolt", func(b *testing.B) {
		tmpDir := b.TempDir()
		bOpts := &bolt.Options{
			NoFreelistSync:  true,
			InitialMmapSize: 64 * 1024 * 1024,
			FreelistType:    bolt.FreelistMapType,
			NoSync:          true,
		}
		dbPath := fmt.Sprintf("%s/metadata.db", tmpDir)
		db, err := bolt.Open(dbPath, 0600, bOpts)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		// Warm up
		if _, err := metadata.NewReader(db, sr, toc); err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			var wg sync.WaitGroup
			durations := make([]time.Duration, parallelism)
			errs := make([]error, parallelism)

			wg.Add(parallelism)
			for p := 0; p < parallelism; p++ {
				go func(idx int) {
					defer wg.Done()
					start := time.Now()
					_, err := metadata.NewReader(db, sr, toc)
					durations[idx] = time.Since(start)
					errs[idx] = err
				}(p)
			}
			wg.Wait()

			for _, e := range errs {
				if e != nil {
					b.Fatalf("NewReader failed: %v", e)
				}
			}

			var total time.Duration
			var max time.Duration
			for _, d := range durations {
				total += d
				if d > max {
					max = d
				}
			}
			mean := total / time.Duration(parallelism)
			b.ReportMetric(float64(mean.Milliseconds()), "ms/mean")
			b.ReportMetric(float64(max.Milliseconds()), "ms/max")
		}
	})

	b.Run("Mem", func(b *testing.B) {
		// Warm up
		if _, err := metadata.NewMemReader(sr, toc); err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			var wg sync.WaitGroup
			durations := make([]time.Duration, parallelism)
			errs := make([]error, parallelism)

			wg.Add(parallelism)
			for p := 0; p < parallelism; p++ {
				go func(idx int) {
					defer wg.Done()
					start := time.Now()
					_, err := metadata.NewMemReader(sr, toc)
					durations[idx] = time.Since(start)
					errs[idx] = err
				}(p)
			}
			wg.Wait()

			for _, e := range errs {
				if e != nil {
					b.Fatalf("NewMemReader failed: %v", e)
				}
			}

			var total time.Duration
			var max time.Duration
			for _, d := range durations {
				total += d
				if d > max {
					max = d
				}
			}
			mean := total / time.Duration(parallelism)
			b.ReportMetric(float64(mean.Milliseconds()), "ms/mean")
			b.ReportMetric(float64(max.Milliseconds()), "ms/max")
		}
	})
}

// Also benchmark read operations (GetAttr, GetChild, ForeachChild, OpenFile)
func benchmarkReads(b *testing.B, parallelism int) {
	toc := buildTOC(5000)
	sr := io.NewSectionReader(zeroReaderAt{}, 0, 1<<30)

	// Create bolt reader
	tmpDir := b.TempDir()
	bOpts := &bolt.Options{
		NoFreelistSync:  true,
		InitialMmapSize: 64 * 1024 * 1024,
		FreelistType:    bolt.FreelistMapType,
		NoSync:          true,
	}
	dbPath := fmt.Sprintf("%s/metadata.db", tmpDir)
	db, err := bolt.Open(dbPath, 0600, bOpts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	boltReader, err := metadata.NewReader(db, sr, toc)
	if err != nil {
		b.Fatal(err)
	}

	// Create mem reader
	memReader, err := metadata.NewMemReader(sr, toc)
	if err != nil {
		b.Fatal(err)
	}

	// Collect some file IDs to read via ForeachChild on root
	type fileInfo struct {
		name string
		id   uint32
	}
	var files []fileInfo
	// Walk one level to get a directory, then get children
	var dirID uint32
	boltReader.ForeachChild(boltReader.RootID(), func(name string, id uint32, mode os.FileMode) bool {
		if mode.IsDir() {
			dirID = id
			return false
		}
		return true
	})
	if dirID != 0 {
		boltReader.ForeachChild(dirID, func(name string, id uint32, mode os.FileMode) bool {
			// walk deeper
			return true
		})
	}
	// Get files from usr/lib directory
	usrID, _, _ := boltReader.GetChild(boltReader.RootID(), "usr")
	if usrID != 0 {
		libID, _, _ := boltReader.GetChild(usrID, "lib")
		if libID != 0 {
			boltReader.ForeachChild(libID, func(name string, id uint32, mode os.FileMode) bool {
				files = append(files, fileInfo{name, id})
				return len(files) < 100
			})
		}
	}

	if len(files) == 0 {
		b.Fatal("no files found for read benchmark")
	}

	readBench := func(b *testing.B, r metadata.Reader, label string) {
		b.Run(label, func(b *testing.B) {
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					f := files[i%len(files)]
					i++
					if _, err := r.GetAttr(f.id); err != nil {
						b.Fatal(err)
					}
					if _, err := r.OpenFile(f.id); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}

	readBench(b, boltReader, "Bolt")
	readBench(b, memReader, "Mem")
}

func BenchmarkBoltVsMem_P3(b *testing.B)  { benchmarkBoltVsMem(b, 3) }
func BenchmarkBoltVsMem_P10(b *testing.B) { benchmarkBoltVsMem(b, 10) }
func BenchmarkReads_P3(b *testing.B)      { benchmarkReads(b, 3) }
func BenchmarkReads_P10(b *testing.B)     { benchmarkReads(b, 10) }
