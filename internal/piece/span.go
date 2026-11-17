package piece

// span is the part of a torrent-global byte range that falls inside one file.
type span struct {
	file    int   // index into the torrent's file list
	fileOff int64 // offset of the span within that file
	bufOff  int64 // offset of the span within the caller's buffer
	length  int64
}

// fileSpans maps the torrent-global byte range [off, off+n) onto files laid
// out back to back with the given lengths. Parts of the range that fall
// beyond the last file produce no span, so callers compare the summed span
// lengths against n to detect a short range.
//
// This is the single place that turns torrent offsets into file offsets;
// writes, upload reads and resume verification all go through it.
func fileSpans(lengths []int64, off, n int64) []span {
	if n <= 0 || off < 0 {
		return nil
	}
	end := off + n
	var spans []span
	var fileStart int64
	for i, length := range lengths {
		fileEnd := fileStart + length
		lo, hi := max(off, fileStart), min(end, fileEnd)
		if lo < hi {
			spans = append(spans, span{
				file:    i,
				fileOff: lo - fileStart,
				bufOff:  lo - off,
				length:  hi - lo,
			})
		}
		if fileEnd >= end {
			break
		}
		fileStart = fileEnd
	}
	return spans
}
