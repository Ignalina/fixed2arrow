package impl

import (
	"fmt"
	"github.com/inhies/go-bytesize"
	"time"
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
