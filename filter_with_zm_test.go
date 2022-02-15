package exp

import (
	log "github.com/sirupsen/logrus"
	"sort"
	"testing"
	"time"
)

func TestZoneMap(t *testing.T) {

}

func BenchmarkComparison(b *testing.B) {
	data := fetchData(5000000, false)
	data2 := fetchData(5000000, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			_ = data[i % 5000000] > data2[i % 5000000]
		}
		//if data[i % 5000000] > data2[i % 5000000] {
		//
		//}
		//if data[i % 5000000] < data2[i % 5000000] {
		//
		//}
		//if data[i % 5000000] > data2[i % 5000000] {
		//
		//}
	}
}

func TestBasic(t *testing.T) {
	N := 40000000
	numSegments := 10
	numParts := 40
	data := fetchData(N, false)
	segments := make([]*Segment, 0)
	for i := 0; i < numSegments; i++ {
		inner := data[i * N / numSegments : (i + 1) * N / numSegments]
		sort.Slice(inner, func(i, j int) bool {
			return inner[i] < inner[j]
		})
		segment := NewSegment(inner, numParts)
		segments = append(segments, segment)
	}

	log.Info("............. setup complete")

	queryTimes := N
	queryData := fetchData(queryTimes, false)

	//queryData = queryData[queryTimes / 10 * 9 :]
	//queryData = queryData[:queryTimes / 10]
	//queryTimes = queryTimes / 10

	start := time.Now()
	for i := 0; i < queryTimes; i++ {
		for k := 0; k < numSegments; k++ {
			if segments[k].Query(queryData[i]) {
				break
			}
			//segments[k].Query(queryData[i])
			//segments[k].Parts[0].Filter.Contains(queryData[i])
			//break
		}
	}
	end := time.Since(start)
	log.Info(float64(end.Nanoseconds()) / float64(queryTimes), " ns/operation")
}

type Segment struct {
	Parts []*Partition
	ZM ZoneMap
}

func NewSegment(data []uint64, numParts int) *Segment {
	parts := make([]*Partition, 0)
	for i := 0; i < numParts; i++ {
		inner := data[i * len(data) / numParts : (i + 1) * len(data) / numParts]
		part := NewPartition(inner)
		parts = append(parts, part)
	}
	zm := newZM(data[0], data[len(data) - 1])
	return &Segment{parts, zm}
}

func (s *Segment) Query(key uint64) bool {
	if key < s.ZM.Min || key > s.ZM.Max {
		return false
	}
	//for _, part := range s.Parts {
	//	if key < part.ZM.Min {
	//		return false
	//	}
	//	if key > part.ZM.Max {
	//		continue
	//	}
	//	if key == part.ZM.Min || key == part.ZM.Max {
	//		return true
	//	}
	//	//return true
	//	return part.Query(key)
	//}

	beg, end := 0, len(s.Parts) - 1
	for beg <= end {
		mid := beg + (end - beg) / 2
		part := s.Parts[mid]
		if key < part.ZM.Min {
			end = mid - 1
			continue
		}
		if key > part.ZM.Max {
			beg = mid + 1
			continue
		}
		if key == part.ZM.Min || key == part.ZM.Max {
			return true
		}
		return part.Query(key)
	}
	return false
}

type Partition struct {
	Data []uint64
	ZM ZoneMap
	Filter *BinaryFuse8
}

func NewPartition(data []uint64) *Partition {
	zm := newZM(data[0], data[len(data) - 1])
	filter, _ := PopulateBinaryFuse8(data)
	return &Partition{
		Data:   data,
		ZM:     zm,
		Filter: filter,
	}
}

func (p *Partition) Query(key uint64) bool {
	//if key < p.ZM.Min || key > p.ZM.Max {
	//	return false
	//}
	return p.Filter.Contains(key)
}

type ZoneMap struct {
	Max uint64
	Min uint64
}

func newZM(min, max uint64) ZoneMap {
	return ZoneMap{max, min}
}
