package main

import (
	"github.com/tarantool/go-tarantool"

	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type calcResult struct {
	smallest     float64
	largest      float64
	sum          float64
	average      float64
	stdDeviation float64
	perSecond    float64
}

const usage string = "usage: ./tarantool -s <server address>\n"

var tarantoolServerAddress string

var testToRun int

var write_wg, read_wg, waitResultsGroup sync.WaitGroup

var calculateResults []calcResult

// Create a shared connection to tarantool server
//
func connectToTarantool(server string) *tarantool.Connection {

	opts := tarantool.Opts{
		Timeout:       1 * time.Second,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
	}

	client, err := tarantool.Connect(server, opts)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	return client
}

// Generate random integer using specified minimum
// and maximum range
//
func random(min, max int) int {
	return rand.Intn(max-min) + min
}

//
// Load specified mock json file from disk folder
func loadJson(index int) []byte {
	jsonFileName := fmt.Sprintf("mocks/input%v.json", index)

	buf, err := ioutil.ReadFile(jsonFileName)
	if err != nil {
		log.Fatalf("Failed to open mock file %s: %s", jsonFileName, err.Error())
	}

	return buf
}

// Load random mock json file from disk folder
//
func loadRandomJson() []byte {
	jsonFileName := fmt.Sprintf("mocks/input%v.json", random(1, 5))

	buf, err := ioutil.ReadFile(jsonFileName)
	if err != nil {
		log.Fatalf("Failed to open mock file %s: %s", jsonFileName, err.Error())
	}

	return buf
}

// Call load object procedure in tarantool. This procedure
// does not require id.
// Params:
// buf - bytes array to save in database
//
func sendObjectWithoutId(client *tarantool.Connection, buf []byte) {
	_, err := client.Call("loadWithoutId", []interface{}{buf})

	if err != nil {
		log.Println("Error when inserting object: ", err)
	}
}

// Call load object procedure in tarantool.
// Params:
// object_id - id of inserted data. Using in tarantoool and in postgres
// saveInCache - boolean, determine should tarantool save object
// buf - bytes array to save in database
//
func sendObjectWithId(client *tarantool.Connection, object_id string, saveInCache bool, buf []byte) {
	_, err := client.Call("loadWithId", []interface{}{object_id, saveInCache, buf})

	if err != nil {
		log.Println("Error when inserting object: ", err)
	}
}

func printResult(testName string, result []calcResult) {

	log.Printf("------------------ Performance result for test %s ------------------\n", testName)

	average := float64(0)
	stdDeviation := float64(0)
	smallest := math.MaxFloat64
	largest := -math.MaxFloat64
	perSecond := float64(0)
	sum := float64(0)

	for round, res := range result {
		log.Printf("----------------------------- Round %d -----------------------------\n", round)

		log.Printf("Average time: %f", res.average)
		log.Printf("Deviation: %f", res.stdDeviation)
		log.Printf("Minimum time: %f", res.smallest)
		log.Printf("Maximum: %f", res.largest)
		log.Printf("Transaction per second: %d", int(res.perSecond))
		log.Printf("All time: %f", res.sum)

		log.Println("-------------------------------------------------------------------")

		average += res.average
		sum += res.sum
		perSecond += res.perSecond

		if res.largest > largest {
			largest = res.largest
		}

		if res.smallest < smallest {
			smallest = res.smallest
		}
	}

	l := float64(len(result))

	log.Println("---------------------------- Total ------------------------------------")

	log.Printf("Average time: %f", average/l)
	log.Printf("Deviation: %f", stdDeviation/l)
	log.Printf("Minimum time: %f", smallest)
	log.Printf("Maximum: %f", largest)
	log.Printf("Transaction per second: %d", int(perSecond/l))
	log.Printf("All time: %f", sum/l)

	log.Println("-----------------------------------------------------------------------")
}

func calculateResult(collectResults []float64) calcResult {
	smallest := math.MaxFloat64
	largest := -math.MaxFloat64
	sum := float64(0)
	sumOfSquares := float64(0)

	for _, result := range collectResults {
		if result > largest {
			largest = result
		}
		if result < smallest {
			smallest = result
		}
		sum += result
		sumOfSquares += result * result
	}

	n := float64(len(collectResults))
	perSecond := n
	average := sum / n
	stdDeviation := math.Sqrt(sumOfSquares/n - (sum/n)*(sum/n))

	if sum > 1 {
		perSecond = n / sum
	}

	result := calcResult{
		smallest:     smallest,
		largest:      largest,
		sum:          sum,
		average:      average,
		stdDeviation: stdDeviation,
		perSecond:    perSecond,
	}

	return result
}

func write_test_worker_without_id(workerCycles int, buf []byte, c chan []float64) {
	defer write_wg.Done()

	client := connectToTarantool(fmt.Sprintf("%s:3013", tarantoolServerAddress))

	collectResults := make([]float64, 0)

	for i := 0; i < workerCycles; i++ {
		start := time.Now()

		sendObjectWithoutId(client, buf)

		elapsed := time.Since(start)
		collectResults = append(collectResults, elapsed.Seconds())
	}

	c <- collectResults
}

func write_test_worker_with_id(workerNumber int, workerCycles int, buf []byte, c chan []float64) {
	defer write_wg.Done()

	client := connectToTarantool(fmt.Sprintf("%s:3013", tarantoolServerAddress))

	collectResults := make([]float64, 0)

	for i := 0; i < workerCycles; i++ {
		start := time.Now()

		sendObjectWithId(client, fmt.Sprintf("%v%v", workerNumber, i), true, buf)

		elapsed := time.Since(start)
		collectResults = append(collectResults, elapsed.Seconds())
	}

	c <- collectResults
}

func getObjectFromTarantool(client *tarantool.Connection, objectId string) {
	resp, err := client.Call("get", []interface{}{objectId})
	if err != nil {
		log.Printf("Get an error when fetching object %s: %s", objectId, err)
	} else {
		// data := resp.Data[0].([]interface{})[0]
		// parse_data := data.(map[interface{}]interface{})
		// json_data := parse_data["data"].(string)

		log.Println("Code", resp.Code)
		log.Println("Data", resp.Data)
	}
}

func read_test_worker(workerNumber int, workerCycles int, c chan []float64) {
	defer read_wg.Done()

	client := connectToTarantool(fmt.Sprintf("%s:3013", tarantoolServerAddress))

	collectResults := make([]float64, 0)

	for i := 0; i < workerCycles; i++ {
		start := time.Now()

		getObjectFromTarantool(client, fmt.Sprintf("%v%v", workerNumber, i))

		elapsed := time.Since(start)
		collectResults = append(collectResults, elapsed.Seconds())
	}

	c <- collectResults
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usage)
		flag.PrintDefaults()
	}

	flag.StringVar(&tarantoolServerAddress, "s", "localhost", "Tarantool server address.")

	flag.Parse()

	if len(flag.Args()) > 1 {
		tarantoolServerAddress = flag.Args()[0]

		i, err := strconv.Atoi(flag.Args()[1])
		if err == nil {
			testToRun = i
		}
	} else if len(flag.Args()) == 1 {
		tarantoolServerAddress = flag.Args()[0]
	}
}

func main() {
	// Initializing random generator
	rand.Seed(time.Now().Unix())

	b := loadJson(0)

	c := make(chan []float64)

	waitResultsGroup.Add(1)

	for i := 1; i < 6; i++ {
		go write_test_worker_without_id(250, b, c)
		write_wg.Add(1)
	}

	go func() {
		defer waitResultsGroup.Done()

		for response := range c {
			calculateResults = append(calculateResults, calculateResult(response))
		}
	}()

	write_wg.Wait()

	close(c)

	waitResultsGroup.Wait()

	printResult("One thread", calculateResults)

	// calculateResults = make([]calcResult, 0)
	// c = make(chan []float64)

	// for i := 1; i < 6; i++ {
	// 	go read_test_worker(250, 1, 250)
	// 	read_wg.Add(1)
	// }

	// // write_wg.Wait()
	// read_wg.Wait()
}
