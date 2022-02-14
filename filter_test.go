package exp

import (
	"encoding/json"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
)

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
	sizes := []int{160000, 500000, 1600000, 5000000, 10000000, 50000000, 100000000}
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
