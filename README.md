# ts-test
## Find the actual activation date of phone number
### Data structure
- Parse data from file to structure `map[uint32]map[uint32]uint32`
- Process and fomat data output structure `map[uint32]uint32`
### Strategy and Algorithm
1. Read CSV file line by line.
2. Parse data from string to uint32 by goroutines (concurrency).
3. Add data to `map[uint32]map[uint32]uint32` (map[<phone number>]map[<start date>]<end date>).
4. Loop each phone number, sort by start date create slice big to small.
5. Find the actual activation date of phone number in sorted slice.
6. Export CSV file result.
### Example

Read data file sample (DataFileSample.csv)
```
PHONE_NUMBER,ACTIVATION_DATE,DEACTIVATION_DATE
0987000001,2016-03-01,2016-05-01
0987000002,2016-02-01,2016-03-01
0987000001,2016-01-01,2016-03-01
0987000001,2016-12-01,
0987000002,2016-03-01,2016-05-01
0987000003,2016-01-01,2016-01-10
0987000001,2016-09-01,2016-12-01
0987000002,2016-05-01,
0987000001,2016-06-01,2016-09-01
```

Parse data to map
```
map[
  987000003:map[1451581200:1452358800] 
  987000001:map[1472662800:1480525200 1456765200:1462035600 1451581200:1456765200 1480525200:2288912640 1464714000:1472662800] 
  987000002:map[1454259600:1456765200 1462035600:2288912640 1456765200:1462035600]
]
```

Loop map by key (phone number), create sorted slice by start date (sort from large to small). Example key 987000001 we have sorted slice
```
[1480525200 1472662800 1464714000 1456765200 1451581200]
```

Loop sorted slice to find the actual activation date of phone number. When `<start date>[i] != <end date>`
```
987000001 1464714000 1462035600
```
This is the result.


