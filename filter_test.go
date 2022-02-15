package exp

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

var bf *BinaryFuse8

var bfs []*BinaryFuse8

func BenchmarkBinaryFuse8Multiple(b *testing.B) {
	N := 40000000
	numFilters := 8
	setupMultiple(N, numFilters, false)
	key := uint64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < numFilters; k++ {
			bfs[k].Contains(key)
		}
		key++
	}
}

func BenchmarkPopulateFuse8SingleParallel(b *testing.B) {
	N := 160000
	sortedGlobal := false
	setupSingle(N, sortedGlobal)
	key := uint64(0)

	b.ResetTimer()
	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bf.Contains(key)
			key++
		}
	})
}

func BenchmarkPopulateFuse8Single(b *testing.B) {
	//log.Info(runtime.GOMAXPROCS(0))
	N := 100000000
	//numFilters := 1
	sortedGlobal := true
	//setupMultiple(N, numFilters, sortedGlobal)
	setupSingle(N, sortedGlobal)
	//key := uint64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//for k := 0; k < numFilters; k++ {
		//	bfs[k].Contains(uint64(i))
		//}
		//bf.Contains(key)
		//key++
		bf.Contains(0)
	}
}

func TestXXX(t *testing.T) {
	N := 1600000
	numFilters := 1
	sortedGlobal := false
	setupMultiple(N, numFilters, sortedGlobal)
	key := rand.Uint64() % uint64(N)

	st := time.Now()
	for k := 0; k < numFilters; k++ {
		bfs[k].Contains(key)
	}
	ed := time.Since(st)
	log.Info(ed.Nanoseconds(), " ns")
}

func TestBinaryFuseFuse8Multiple(t *testing.T) {
	N := 32000000
	concurrently := false
	numFilters := 20
	sortedGlobal := false
	batchSize := 1600000
	queryData := fetchData(N, false)
	//queryData = make([]uint64, 1000)
	setupMultiple(N, numFilters, sortedGlobal)

	//querySingleThread := func(key uint64) {
	//	for k := 0; k < numFilters; k++ {
	//		bfs[k].Contains(key)
	//	}
	//}

	queryMultiThreads := func(key uint64) {
		var wg sync.WaitGroup
		wg.Add(numFilters)
		for k := 0; k < numFilters; k++ {
			go func(k int) {
				bfs[k].Contains(key)
				wg.Done()
			}(k)
		}
		wg.Wait()
	}

	if concurrently {
		start := time.Now()
		for i := 0; i < len(queryData); i++ {
			queryMultiThreads(queryData[i])
		}
		end := time.Since(start)
		log.Info(float64(end.Nanoseconds()) / float64(len(queryData)), " ns/operation")
	} else {
		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			for i := 0; i < len(queryData) / batchSize; i++ {
				//querySingleThread(queryData[i])
				data := queryData[i * batchSize : (i + 1) * batchSize]
				for k := 0; k < numFilters; k++ {
					for j := 0; j < batchSize; j++ {
						bfs[k].Contains(data[j])
					}
				}
			}

			//for k := 0; k < numFilters; k++ {
			//	for i := 0; i < len(queryData); i++ {
			//		bfs[k].Contains(queryData[i])
			//	}
			//}
			wg.Done()
		}()
		wg.Wait()
		end := time.Since(start)
		log.Info(float64(end.Nanoseconds()) / float64(len(queryData)), " ns/operation")
	}
}

func TestBinaryFuse8Single(t *testing.T) {
	N := 5000000
	numThreads := 1
	setupSingle(N, true)
	queryData := fetchData(N * 20, false)
	//for i := 0; i < len(queryData); i++ {
	//	queryData[i] += uint64(N)
	//}

	var wg sync.WaitGroup
	reader := func(data []uint64) {
		for i := 0; i < len(data); i++ {
			bf.Contains(queryData[i])
		}
		wg.Done()
	}

	start := time.Now()
	for k := 0; k < numThreads; k++ {
		data := queryData[k * len(queryData) / numThreads: (k + 1) * len(queryData) / numThreads]
		wg.Add(1)
		go reader(data)
	}
	wg.Wait()
	end := time.Since(start)

	log.Info(float64(end.Nanoseconds()) / float64(len(queryData)), " ns/operation")
}

func setupSingle(N int, sorted bool) {
	if bf != nil {
		return
	}
	log.Info("setup filter begin ...... size: ", N)
	bf, _ = PopulateBinaryFuse8(fetchData(N, sorted))
	log.Info("setup filter end ...... size: ", N)
}

func setupMultiple(N int, numFilters int, sortedGlobal bool) {
	if len(bfs) != 0 {
		return
	}
	log.Info("setup filters begin ...... size: ", N, " numFilters: ", numFilters)
	dataTotal := fetchData(N, sortedGlobal)
	bfs = make([]*BinaryFuse8, 0)
	for i := 0; i < numFilters; i++ {
		dataLocal := dataTotal[i * N / numFilters : (i + 1) * N / numFilters]
		bf, _ := PopulateBinaryFuse8(dataLocal)
		bfs = append(bfs, bf)
	}
	log.Info("setup filters end ...... size: ", N, " numFilters: ", numFilters)
}

func fetchData(N int, sorted bool) []uint64 {
	if sorted {
		filename := "/Users/asuka/filterexp/data/" + strconv.Itoa(N) + ".sorted"
		f, _ := os.Open(filename)
		stat, _ := f.Stat()
		buf := make([]byte, stat.Size())
		f.Read(buf)
		var data []uint64
		json.Unmarshal(buf, &data)
		return data
	} else {
		filename := "/Users/asuka/filterexp/data/" + strconv.Itoa(N) + ".random"
		f, _ := os.Open(filename)
		stat, _ := f.Stat()
		buf := make([]byte, stat.Size())
		f.Read(buf)
		var data []uint64
		json.Unmarshal(buf, &data)
		return data
	}
}

func TestFetchData(t *testing.T) {
	data := fetchData(160000, true)
	t.Log(data[:10])
	data = fetchData(160000, false)
	t.Log(data[:10])
}

func genData(N int, sorted bool) []uint64 {
	dedup := make(map[int]bool)
	data := make([]uint64, 0, N)
	for i := 0; i < N; i++ {
		tmp := rand.Intn(N)
		for {
			if _, ok := dedup[tmp]; ok {
				tmp = rand.Intn(N)
				continue
			}
			dedup[tmp] = true
			data = append(data, uint64(tmp))
			break
		}
	}
	if sorted {
		sort.Slice(data, func(i, j int) bool {
			return data[i] < data[j]
		})
	}
	return data
}

func TestDataGenerator(t *testing.T) {
	sizes := []int{100000/*160000, 500000, 1600000, 5000000, 10000000, 50000000, 100000000*/}
	for _, size := range sizes {
		data := genData(size, true)
		filename := "/Users/asuka/filterexp/data/" + strconv.Itoa(size) + ".sorted"
		f, _ := os.Create(filename)
		buf, _ := json.Marshal(data)
		f.Write(buf)
		f.Close()

		data = genData(size, false)
		filename = "/Users/asuka/filterexp/data/" + strconv.Itoa(size) + ".random"
		f, _ = os.Create(filename)
		buf, _ = json.Marshal(data)
		f.Write(buf)
		f.Close()
	}
}

func TestGenData(t *testing.T) {
	arr := genData(10, false)
	t.Log(arr)
	arr = genData(10, true)
	t.Log(arr)
}
