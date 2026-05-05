package streamer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type LogStreamer struct {
	logDir string
}

func New(logDir string) *LogStreamer {
	return &LogStreamer{logDir: logDir}
}

func (s *LogStreamer) logPath(runID string) string {
	return filepath.Join(s.logDir, runID+".log")
}

func (s *LogStreamer) Writer(runID string) (io.WriteCloser, error) {
	if err := os.MkdirAll(s.logDir, 0755); err != nil {
		return nil, fmt.Errorf("creating log dir: %w", err)
	}
	f, err := os.OpenFile(s.logPath(runID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening log file for write: %w", err)
	}
	return f, nil
}

func (s *LogStreamer) Reader(runID string) (io.ReadCloser, error) {
	f, err := os.Open(s.logPath(runID))
	if err != nil {
		return nil, fmt.Errorf("opening log file for read: %w", err)
	}
	return f, nil
}

func (s *LogStreamer) Tail(ctx context.Context, runID string, w io.Writer) error {
	f, err := os.Open(s.logPath(runID))
	if err != nil {
		return fmt.Errorf("opening log file for tail: %w", err)
	}
	defer f.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	buf := make([]byte, 4096)
	var lineBuf []byte

	for {
		for {
			n, readErr := f.Read(buf)
			if n > 0 {
				lineBuf = append(lineBuf, buf[:n]...)
				for {
					idx := bytes.IndexByte(lineBuf, '\n')
					if idx == -1 {
						break
					}
					w.Write(lineBuf[:idx+1])
					lineBuf = lineBuf[idx+1:]
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}

		select {
		case <-ctx.Done():
			if len(lineBuf) > 0 {
				w.Write(lineBuf)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
