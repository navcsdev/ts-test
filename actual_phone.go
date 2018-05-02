package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
	"unsafe"
)

type ByDate []uint32

func (a ByDate) Len() int           { return len(a) }
func (a ByDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool { return a[i] > a[j] }

var mapInput map[uint32]map[uint32]uint32
var mapOutput map[uint32]uint32

func main() {
	start := time.Now()

	ReadCsvFile("DataFileSample.csv")
	findActualPhone()
	WriteCsvOutput()
	// WriteCsvFileTestSample()

	elapsed := time.Since(start)
	log.Printf("Excuted time %s", elapsed)
}

func ReadCsvFile(pathFile string) {
	mapInput = make(map[uint32]map[uint32]uint32)
	mapOutput = make(map[uint32]uint32)
	cSendLine := make(chan func() (uint32, uint32, uint32), 1000)

	csvFile, _ := os.Open(pathFile)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	reader.Read() // next header
	goroutine := 0

	for {
		line, error := reader.Read()
		goroutine++

		if error == io.EOF {
			for i := 0; i < goroutine-1; i++ {
				addToMap((<-cSendLine)())
			}
			break
		} else if error != nil {
			log.Fatal(error)
		}

		go parseLine(line, cSendLine)
		if goroutine%1000 == 0 {
			goroutine -= 1000
			for i := 0; i < 1000; i++ {
				addToMap((<-cSendLine)())
			}
		}
	}

}

func findActualPhone() {
	for phone, datesMap := range mapInput {
		startDates := sortStartDates(datesMap)

		if len(startDates) == 1 {
			mapOutput[phone] = startDates[0]
		} else {
			idxActualDate := findIdxActualDate(startDates, datesMap)
			mapOutput[phone] = startDates[idxActualDate]
		}
		delete(mapInput, phone)
	}
}

func findIdxActualDate(startDates ByDate, datesMap map[uint32]uint32) (idxActualDate int) {
	for i := 0; i < len(startDates)-1; i++ {
		if startDates[i] != datesMap[startDates[i+1]] {
			idxActualDate = i
			break
		} else {
			idxActualDate = i + 1
		}
	}
	return
}

func sortStartDates(datesMap map[uint32]uint32) (startDates ByDate) {
	for startDate := range datesMap {
		startDates = append(startDates, startDate)
	}
	sort.Sort(startDates)
	return
}

func parseLine(line []string, cSendLine chan func() (uint32, uint32, uint32)) {
	phone, err := strconv.ParseUint(line[0], 10, 32)
	if err != nil {
		log.Fatal(err)
		return
	}

	startDate, err := time.Parse(time.RFC3339, line[1]+"T0:00:00+07:00")
	if err != nil {
		log.Fatal(err)
		return
	}

	endDate, err := time.Parse(time.RFC3339, line[2]+"T0:00:00+07:00")

	cSendLine <- (func() (uint32, uint32, uint32) {
		return uint32(phone), uint32(startDate.Unix()), uint32(endDate.Unix())
	})
}

func addToMap(phone uint32, startDate uint32, endDate uint32) {
	if mapDates, ok := mapInput[phone]; ok {
		mapDates[startDate] = endDate
	} else {
		mapInput[phone] = make(map[uint32]uint32)
		mapInput[phone][startDate] = endDate
	}
}

func WriteCsvOutput() {
	file, err := os.Create("Output.csv")
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"PHONE_NUMBER", "REAL_ACTIVATION_DATE"})

	for key, value := range mapOutput {
		line := []string{"0" + String(key), time.Unix(int64(value), 0).Format("2006-01-02")}
		err := writer.Write(line)
		checkError("Cannot write to file", err)
	}
}

func String(n uint32) string {
	buf := [11]byte{}
	pos := len(buf)
	i := int64(n)
	signed := i < 0
	if signed {
		i = -i
	}
	for {
		pos--
		buf[pos], i = '0'+byte(i%10), i/10
		if i == 0 {
			if signed {
				pos--
				buf[pos] = '-'
			}
			return string(buf[pos:])
		}
	}
}

var dataInCsvFile = [][]string{
	{"PHONE_NUMBER", "ACTIVATION_DATE", "DEACTIVATION_DATE"},
	{"0987000001", "2016-03-01", "2016-05-01"},
}

func WriteCsvFileTestSample() {
	file, err := os.Create("DataFileSample.csv")
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range dataInCsvFile {
		err := writer.Write(value)
		checkError("Cannot write to file", err)
	}

	oneLine := []string{"0987000001", "2016-03-01", "2016-05-01"}
	fmt.Printf("size one line %d len %d\n", unsafe.Sizeof(oneLine), len(oneLine))

	for i := 0; i < 50000000; i++ {
		err := writer.Write(oneLine)
		checkError("Cannot write to file", err)
	}
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
