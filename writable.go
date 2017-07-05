package sequencefile

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	BytesWritableClassName = "org.apache.hadoop.io.BytesWritable"
	TextClassName          = "org.apache.hadoop.io.Text"
	IntWritableClassName   = "org.apache.hadoop.io.IntWritable"
	LongWritableClassName  = "org.apache.hadoop.io.LongWritable"
	NullWritableClassName  = "org.apache.hadoop.io.NullWritable"
)

// BytesWritable unwraps a hadoop BytesWritable and returns the actual bytes.
func BytesWritable(b []byte) []byte {
	// Guard against NullWritable values
	if len(b) > 4 {
		return b[4:]
	}

	return []byte{}
}

func PutBytesWritable(raw []byte) []byte {
	serialized := make([]byte, 4, 4+len(raw))
	binary.BigEndian.PutUint32(serialized, uint32(len(raw)))
	serialized = append(serialized, raw...)
	return serialized
}

// Text unwraps a Text and returns the deserialized string.
func Text(b []byte) string {
	buf := bytes.NewBuffer(b)
	n, err := ReadVInt(buf)
	if err != nil {
		panic(fmt.Sprintf("sequencefile: unwrapping Text: %s", err))
	}

	if int(n) != buf.Len() {
		panic("sequencefile: unwrapping Text: bad length")
	}

	return string(buf.Bytes())
}

// IntWritable unwraps an IntWritable and returns the deserialized int32.
func IntWritable(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

// LongWritable unwraps an LongWritable and returns the deserialized int64.
func LongWritable(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
