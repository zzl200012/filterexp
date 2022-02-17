package exp

import (
	log "github.com/sirupsen/logrus"
	"sort"
	"testing"
	"time"
)

func TestPureMemoryDeduplication(t *testing.T) {
	rowsPerSeg := 3200000
	numSeg := 20
	N := numSeg * rowsPerSeg


	source := fetchData(N, true)
	segments := make([]*Seg, 0)
	for i := 0; i < numSeg; i++ {
		inner := source[i * N / numSeg : (i + 1) * N / numSeg]
		seg := newSeg(inner)
		segments = append(segments, seg)
	}
	log.Info("env generated successfully | layout: sorted")

	// query data is sorted
	query := fetchData(N, true)
	start := time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSeg; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end := time.Since(start)
	log.Info("source sorted | query sorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// query data is unsorted
	query = fetchData(N, false)
	start = time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSeg; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end = time.Since(start)
	log.Info("source sorted | query unsorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// source data is global unsorted
	source = fetchData(N, false)
	segments = make([]*Seg, 0)
	for i := 0; i < numSeg; i++ {
		inner := source[i * N / numSeg : (i + 1) * N / numSeg]
		sort.Slice(inner, func(i, j int) bool {
			return inner[i] < inner[j]
		})
		seg := newSeg(inner)
		segments = append(segments, seg)
	}
	log.Info("env generated successfully | layout: unsorted")

	// query data is sorted
	query = fetchData(N, true)
	start = time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSeg; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end = time.Since(start)
	log.Info("source unsorted | query sorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// query data is unsorted
	query = fetchData(N, false)
	start = time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSeg; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end = time.Since(start)
	log.Info("source unsorted | query unsorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

}

type Seg struct {
	data []uint64
	filter *BinaryFuse8
	zm ZoneMap
}

func newSeg(data []uint64) *Seg {
	f, _ := PopulateBinaryFuse8(data)
	zm := newZM(data[0], data[len(data) - 1])
	return &Seg{
		data:   data,
		filter: f,
		zm:     zm,
	}
}

func (s *Seg) query(key uint64) bool {
	if key < s.zm.Min || key > s.zm.Max {
		return false
	}
	return s.filter.Contains(key)
}


