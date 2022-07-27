package impl

import (
	"fmt"
	"github.com/inhies/go-bytesize"
	"runtime"
	"time"
	"unicode/utf8"
)

func PrintPerfomance(elapsed time.Duration, fst *FixedSizeTable) {

	fcores := float64(fst.Cores)
	var tpb = bytesize.New(float64(len(fst.Bytes)) / float64(elapsed.Seconds()))
	var tpl = bytesize.New(float64(fst.LinesParsed) / float64(elapsed.Seconds()))
	tpls := tpl.String()[:len(tpl.String())-1]
	toAvro := fst.DurationToArrow.Seconds() / fcores
	tpal := bytesize.New(float64(fst.LinesParsed) / toAvro)
	tpals := tpal.String()[:len(tpal.String())-1]

	fmt.Println("Time spend in total     :", elapsed, " parsing ", fst.LinesParsed, " lines from ", len(fst.Bytes), " bytes")

	fmt.Println("Troughput bytes/s total :", tpb, "/s")
	fmt.Println("Troughput lines/s total :", tpls, " Lines/s")
	fmt.Println("Troughput lines/s toArrow:", tpals, " Lines/s")

	fmt.Println("Time spent toReadChunks :", fst.DurationReadChunk.Seconds()/fcores, "s")
	fmt.Println("Time spent toArrow       :", toAvro, "s")
	fmt.Println("Time spent WaitDoneExport      :", fst.DurationDoneExport.Seconds(), "s")

}

// Borrowed this code from https://golangcode.com/print-the-current-memory-usage/
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

type Substring struct {
	RuneLen int
	Sub     string
	Null    bool
}

func CreateSubstring(fst *FixedSizeTable) []Substring {

	substring := make([]Substring, len(fst.Row.FixedField))
	for ci, cc := range fst.Row.FixedField {
		substring[ci].RuneLen = cc.Len
	}

	return substring
}

func GetSplitBytePositions(fullString string, substring []Substring) {

	var firstByte, lastByte int
	lastByte = len(fullString)

	for is, s := range substring {

		var runeLen int

		for bytePos, runan := range fullString[firstByte:lastByte] {
			runeLen++
			if runeLen == s.RuneLen {
				pos := firstByte + bytePos + utf8.RuneLen(runan)
				substring[is].Sub = fullString[firstByte:pos]
				firstByte = pos
				break
			}
		}

	}
}
