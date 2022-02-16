package exp

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
)

func TestPKDeduplication(t *testing.T) {
	rowsPerBlock := 160000
	blocksPerSeg := 40
	numSegments := 30
	rowsPerSeg := rowsPerBlock * blocksPerSeg
	N := rowsPerSeg * numSegments
	log.Info("Total: ", N, " rows")
	log.Info("       ", numSegments, " segments")

	// source data is global sorted
	source := fetchData(N, true)
	segments := make([]*segment, 0)
	for i := 0; i < numSegments; i++ {
		inner := source[i * N / numSegments : (i + 1) * N / numSegments]
		//sort.Slice(inner, func(i, j int) bool {
		//	return inner[i] < inner[j]
		//})
		seg := getSegment(inner, blocksPerSeg)
		segments = append(segments, seg)
	}
	log.Info("env generated successfully | layout: sorted")

	// query data is sorted
	query := fetchData(N, true)
	start := time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSegments; k++ {
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
		for k := 0; k < numSegments; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end = time.Since(start)
	log.Info("source sorted | query unsorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")

	// source data is global unsorted
	source = fetchData(N, false)
	segments = make([]*segment, 0)
	for i := 0; i < numSegments; i++ {
		inner := source[i * N / numSegments : (i + 1) * N / numSegments]
		sort.Slice(inner, func(i, j int) bool {
			return inner[i] < inner[j]
		})
		seg := getSegment(inner, blocksPerSeg)
		segments = append(segments, seg)
	}
	log.Info("env generated successfully | layout: unsorted")

	// query data is sorted
	query = fetchData(N, true)
	start = time.Now()
	for i := 0; i < N; i++ {
		for k := 0; k < numSegments; k++ {
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
		for k := 0; k < numSegments; k++ {
			if segments[k].query(query[i]) {
				break
			}
		}
	}
	end = time.Since(start)
	log.Info("source unsorted | query unsorted | ", float64(end.Nanoseconds()) / float64(N), " ns/operation")
}

func TestBase(t *testing.T) {
	data := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	numBlocks := 5
	s := getSegment(data, numBlocks)
	for _, blk := range s.blocks {
		log.Info(blk.Data)
	}

	assert.True(t, s.query(3))
	assert.False(t, s.query(10))

	log.Info(".......")

	data = []uint64{2, 4, 1, 6, 7, 3, 5, 9, 8, 0}
	s = getSegment(data, numBlocks)
	for _, blk := range s.blocks {
		log.Info(blk.Data)
	}
}

// segment contains 40 blocks and 1 global zone map
type segment struct {
	blocks []*block
	ZM ZoneMap
}

// getSegment requires the data to be sorted
func getSegment(data []uint64, numBlocks int) *segment {
	blks := make([]*block, 0)
	for i := 0; i < numBlocks; i++ {
		inner := data[i * len(data) / numBlocks : (i + 1) * len(data) / numBlocks]
		blk := getBlock(inner)
		blks = append(blks, blk)
	}
	zm := newZM(data[0], data[len(data) - 1])
	return &segment{blks, zm}
}

func (seg *segment) query(key uint64) bool {
	if key < seg.ZM.Min || key > seg.ZM.Max {
		return false
	}
	// binary search the zone map
	beg, end := 0, len(seg.blocks) - 1
	for beg <= end {
		mid := beg + (end - beg) / 2
		blk := seg.blocks[mid]
		if key < blk.ZM.Min {
			end = mid - 1
			continue
		}
		if key > blk.ZM.Max {
			beg = mid + 1
			continue
		}
		if key == blk.ZM.Min || key == blk.ZM.Max {
			return true
		}
		return blk.query(key)
	}
	return false
}

// block contains 160000 rows and 1 zone map
type block struct {
	Data []uint64
	ZM ZoneMap
	Filter *BinaryFuse8
}

func getBlock(data []uint64) *block {
	zm := newZM(data[0], data[len(data) - 1])
	filter, _ := PopulateBinaryFuse8(data)
	return &block {
		Data:   data,
		ZM:     zm,
		Filter: filter,
	}
}

func (blk *block) query(key uint64) bool {
	return blk.Filter.Contains(key)
}
