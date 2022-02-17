package exp

import (
	log "github.com/sirupsen/logrus"
	"sort"
	"testing"
	"time"
)

func TestPureMemoryDeduplication(t *testing.T) {
	rowsPerSeg := 6400000
	numSeg := 10
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
	filterCount = 0
	zmCount = 0
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
	log.Info("zm count: ", zmCount)
	log.Info("filter count: ", filterCount)
	log.Info("source sorted | query sorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// query data is unsorted
	filterCount = 0
	zmCount = 0
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
	log.Info("zm count: ", zmCount)
	log.Info("filter count: ", filterCount)
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
	filterCount = 0
	zmCount = 0
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
	log.Info("zm count: ", zmCount)
	log.Info("filter count: ", filterCount)
	log.Info("source unsorted | query sorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// query data is unsorted
	filterCount = 0
	zmCount = 0
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
	log.Info("zm count: ", zmCount)
	log.Info("filter count: ", filterCount)
	log.Info("source unsorted | query unsorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// all query data does not exist
	filterCount = 0
	zmCount = 0
	query = fetchData(N, false)
	for i := 0; i < len(query); i++ {
		query[i] += uint64(N)
	}
	start = time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSeg; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end = time.Since(start)
	log.Info("zm count: ", zmCount)
	log.Info("filter count: ", filterCount)
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
		zmCount++
		return false
	}
	filterCount++
	return s.filter.Contains(key)
}


