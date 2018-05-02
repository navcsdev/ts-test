package main

import (
	"reflect"
	"testing"
)

func TestReadCsvFileDataSample(t *testing.T) {
	ReadCsvFile("DataFileSample.csv")
	result := map[uint32]map[uint32]uint32{
		987000001: {1451581200: 1456765200, 1472662800: 1480525200, 1480525200: 2288912640, 1464714000: 1472662800, 1456765200: 1462035600},
		987000002: {1454259600: 1456765200, 1456765200: 1462035600, 1462035600: 2288912640},
		987000003: {1451581200: 1452358800},
	}

	twoMapEqual := reflect.DeepEqual(mapInput, result)

	if !twoMapEqual {
		t.Errorf("Test failed, expected: '%t', got: '%t'", true, twoMapEqual)
	}
}
func TestSortStartDates(t *testing.T) {
	dataTest := map[uint32]uint32{1464714000: 1472662800, 1472662800: 1480525200, 1480525200: 2288912640, 1456765200: 1462035600, 1451581200: 1456765200}
	sorted := sortStartDates(dataTest)
	sortedExpect := []uint32{1480525200, 1472662800, 1464714000, 1456765200, 1451581200}

	resultTest := true

	if sorted == nil && sortedExpect == nil {
		resultTest = true
	}

	if sorted == nil || sortedExpect == nil {
		resultTest = false
	}

	if len(sorted) != len(sortedExpect) {
		resultTest = false
	}

	for i := range sorted {
		if sorted[i] != sortedExpect[i] {
			resultTest = false
		}
	}

	if !resultTest {
		t.Errorf("Test failed, expected: '%t', got: '%t'", true, resultTest)
	}
}

func TestFindIdxActualDate(t *testing.T) {
	datesMap := map[uint32]uint32{1464714000: 1472662800, 1472662800: 1480525200, 1480525200: 2288912640, 1456765200: 1462035600, 1451581200: 1456765200}
	startDates := sortStartDates(datesMap)
	idxActualDate := findIdxActualDate(startDates, datesMap)

	if idxActualDate != 2 {
		t.Errorf("Test failed, expected: '%d', got: '%d'", 2, idxActualDate)
	}
}
