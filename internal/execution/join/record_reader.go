package join

import "encoding/binary"

// recordReader borrows a collection's immutable buffers. The owner must not
// append, resize or release the collection until all reads and emitted borrowed
// rows have finished. Resident partitions are frozen by build finalization;
// spilled partitions are frozen after loadBuild.
type recordReader struct {
	data, offsets []byte
	count         int
}

func (c *recordCollection) borrowReader() recordReader {
	if c == nil || c.data == nil || c.offsets == nil {
		return recordReader{}
	}
	return recordReader{data: c.data.Bytes()[:c.used], offsets: c.offsets.Bytes()[:(c.count+1)*4], count: c.count}
}

func (r recordReader) record(i int) (uint64, []byte, bool) {
	if i < 0 || i >= r.count {
		return 0, nil, false
	}
	start := int(binary.LittleEndian.Uint32(r.offsets[i*4:]))
	end := int(binary.LittleEndian.Uint32(r.offsets[(i+1)*4:]))
	if start < 0 || end < start+12 || end > len(r.data) {
		return 0, nil, false
	}
	record := r.data[start:end]
	if int(binary.LittleEndian.Uint32(record[8:12])) != len(record)-12 {
		return 0, nil, false
	}
	return binary.LittleEndian.Uint64(record[:8]), record[12:], true
}
