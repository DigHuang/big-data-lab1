package mr

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 排序将 reduce 任务的 key 进行排序
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, file := range files {
		file, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

// 获取 reduce 任务的 FileName
func selectReduceFiles(reduceNum int) []string {
	s := []string{}
	path, _ := os.Getwd() // 获取当前路径
	files, _ := os.ReadDir(path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-") && strings.HasSuffix(f.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, f.Name())
		}
	}
	return s
}
