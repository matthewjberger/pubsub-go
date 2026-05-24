package pubsub

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// MaxFrameSize is the largest single JSON frame this implementation will
// accept on a read. Frames longer than this are rejected outright so a
// hostile or buggy peer cannot force the broker to allocate gigabytes.
const MaxFrameSize = 16 * 1024 * 1024

// encodeFrame JSON-encodes value into a single length-prefixed frame: a
// 4-byte big-endian length followed by the JSON body. The broker encodes a
// fan-out frame once with this and writes the same bytes to every subscriber,
// so a publish to N subscribers marshals once rather than N times.
func encodeFrame(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("pubsub: marshal frame: %w", err)
	}
	if len(body) > MaxFrameSize {
		return nil, fmt.Errorf("pubsub: frame too large: %d bytes > %d", len(body), MaxFrameSize)
	}
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[:4], uint32(len(body)))
	copy(out[4:], body)
	return out, nil
}

// WriteFrame JSON-encodes value and writes it framed: a 4-byte big-endian
// length prefix followed by the JSON body. One call writes one complete
// frame. The write is performed as a single Write to minimise the number of
// short writes the OS sees; callers wrapping w in a [bufio.Writer] should
// flush after the frame.
func WriteFrame(writer io.Writer, value any) error {
	out, err := encodeFrame(value)
	if err != nil {
		return err
	}
	if _, err := writer.Write(out); err != nil {
		return fmt.Errorf("pubsub: write frame: %w", err)
	}
	return nil
}

// ReadFrame reads one length-prefixed JSON frame from reader and decodes it
// into target, which must be a non-nil pointer. The reader is typically a
// [bufio.Reader] over the TCP connection. Returns [io.EOF] verbatim when the
// peer closes cleanly between frames.
func ReadFrame(reader *bufio.Reader, target any) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return fmt.Errorf("pubsub: zero-length frame")
	}
	if length > MaxFrameSize {
		return fmt.Errorf("pubsub: frame too large: %d bytes > %d", length, MaxFrameSize)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return fmt.Errorf("pubsub: read frame body: %w", err)
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("pubsub: unmarshal frame: %w", err)
	}
	return nil
}
