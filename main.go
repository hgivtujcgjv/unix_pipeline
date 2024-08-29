package main

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type job func(in, out chan interface{})

const (
	MaxInputDataLen = 100
)

var (
	dataSignerOverheat uint32 = 0
	DataSignerSalt            = ""
)

var OverheatLock = func() {
	for {
		if swapped := atomic.CompareAndSwapUint32(&dataSignerOverheat, 0, 1); !swapped {
			fmt.Println("OverheatLock happened")
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

var OverheatUnlock = func() {
	for {
		if swapped := atomic.CompareAndSwapUint32(&dataSignerOverheat, 1, 0); !swapped {
			fmt.Println("OverheatUnlock happened")
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

var DataSignerMd5 = func(data string) string {
	OverheatLock()
	defer OverheatUnlock()
	data += DataSignerSalt
	dataHash := fmt.Sprintf("%x", md5.Sum([]byte(data)))
	time.Sleep(10 * time.Millisecond)
	return dataHash
}

var DataSignerCrc32 = func(data string) string {
	data += DataSignerSalt
	crcH := crc32.ChecksumIEEE([]byte(data))
	dataHash := strconv.FormatUint(uint64(crcH), 10)
	time.Sleep(time.Second)
	return dataHash
}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	for _, i := range jobs {
		out := make(chan interface{})
		wg.Add(1)
		go workerJob(i, in, out, wg)
		in = out
	}
	wg.Wait()
}

func workerJob(tjb job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)
	tjb(in, out)
}

//crc32(data)+"~"+crc32(md5(data))

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for i := range in {
		wg.Add(1)
		go SingleHashJob(i, out, wg, mu)
	}
	wg.Wait()
}
func SingleHashJob(i interface{}, out chan interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()
	data := strconv.Itoa(i.(int))
	mu.Lock()
	res_md5 := DataSignerMd5(data)
	mu.Unlock()
	crc32 := make(chan string)
	go dstcrc32(data, crc32)
	data2 := <-crc32
	res_md_crc := DataSignerCrc32(res_md5)
	fmt.Println("SingleHash result:", res_md5+"~"+res_md_crc)
	out <- data2 + "~" + res_md_crc
}
func dstcrc32(data string, out chan string) {
	out <- DataSignerCrc32(data)
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	const TH int = 6
	for i := range in {
		wg.Add(1)
		go MultieHashJob(i.(string), out, wg, TH)
	}
	wg.Wait()
}

func MultieHashJob(i string, out chan interface{}, wg *sync.WaitGroup, TH int) {
	defer wg.Done()
	mu := &sync.Mutex{}
	nwg := &sync.WaitGroup{}
	totlchnks := make([]string, TH)
	for th := 0; th < TH; th++ {
		nwg.Add(1)
		data := i + strconv.Itoa(th)
		go func(get_str []string, indx int, data string, wg *sync.WaitGroup, mu *sync.Mutex) {
			defer wg.Done()
			data2 := DataSignerCrc32(data)
			mu.Lock()
			get_str[indx] = data2
			mu.Unlock()
		}(totlchnks, th, data, nwg, mu)
	}
	nwg.Wait()
	fmt.Println("MultiHash result:", strings.Join(totlchnks, ""))
	out <- strings.Join(totlchnks, "")
}

func CombineResults(in, out chan interface{}) {
	var res []string
	for i := range in {
		res = append(res, i.(string))
	}

	sort.Strings(res)
	fmt.Println("CombineResults:", strings.Join(res, "_"))
	out <- strings.Join(res, "")
}

func main() {
	fmt.Println("Starting pipeline execution...")
	inputData := []int{0, 1, 1, 2, 3, 5}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		SingleHash,
		MultiHash,
		CombineResults,
		job(func(in, out chan interface{}) {
			result := <-in
			fmt.Println("Pipeline result:", result)
		}),
	}
	start := time.Now()
	ExecutePipeline(hashSignJobs...)
	end := time.Since(start)

	expectedTime := time.Second * 4

	if end > expectedTime {
		panic("Время превышено")
	}
}
