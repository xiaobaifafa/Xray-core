//go:build linux || android

package proxy

import (
	"context"
	"io"
	"math"
	"syscall"

	"github.com/xtls/xray-core/common/net"
	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"
)

const maxRawSpliceChunk = 1 << 20

// copyRawConnWithSplice forwards a raw stream through a kernel pipe. When a
// limiter is present, it charges each chunk before the pipe is drained to the
// destination. This keeps the zero-copy splice path while enforcing the same
// shared per-user token bucket used by RateLimitWriter.
func copyRawConnWithSplice(
	ctx context.Context,
	readerConn net.Conn,
	writerConn net.Conn,
	limiter *rate.Limiter,
	onStart func(),
) (written int64, handled bool, err error) {
	src, ok := readerConn.(syscall.Conn)
	if !ok {
		return 0, false, nil
	}
	dst, ok := writerConn.(syscall.Conn)
	if !ok {
		return 0, false, nil
	}

	srcRaw, err := src.SyscallConn()
	if err != nil {
		return 0, false, err
	}
	dstRaw, err := dst.SyscallConn()
	if err != nil {
		return 0, false, err
	}

	if limiter != nil && limiter.Burst() <= 0 {
		return 0, true, io.ErrShortWrite
	}

	var pipeFD [2]int
	if err := unix.Pipe2(pipeFD[:], unix.O_CLOEXEC|unix.O_NONBLOCK); err != nil {
		return 0, false, err
	}
	defer unix.Close(pipeFD[0])
	defer unix.Close(pipeFD[1])

	// A larger pipe reduces syscall frequency. This is an optimization only;
	// the loop remains correct if the kernel keeps the default pipe capacity.
	_, _ = unix.FcntlInt(uintptr(pipeFD[1]), unix.F_SETPIPE_SZ, maxRawSpliceChunk)

	started := false
	for {
		// Recalculate the chunk size so live limit changes can also increase
		// the splice chunk and avoid permanently paying small-chunk overhead.
		chunkSize := rawSpliceChunkSize(limiter)
		var inPipe int64
		var eof bool
		var sourceErr error

		err := srcRaw.Read(func(fd uintptr) bool {
			for {
				n, spliceErr := unix.Splice(int(fd), nil, pipeFD[1], nil, chunkSize, unix.SPLICE_F_NONBLOCK)
				if spliceErr == unix.EINTR {
					continue
				}
				if spliceErr == unix.EAGAIN || spliceErr == unix.EWOULDBLOCK {
					return false
				}
				if spliceErr != nil {
					sourceErr = spliceErr
					return true
				}
				if n == 0 {
					eof = true
					return true
				}
				inPipe = n
				return true
			}
		})
		if err != nil {
			return written, true, err
		}
		if sourceErr != nil {
			// EINVAL means this source socket cannot be spliced. No bytes from
			// this attempt were consumed, and the pipe is empty, so the caller
			// can safely continue with its buffered reader.
			if sourceErr == unix.EINVAL {
				return written, false, nil
			}
			return written, true, sourceErr
		}
		if eof {
			return written, true, nil
		}

		if !started {
			started = true
			if onStart != nil {
				onStart()
			}
		}

		if limiter != nil {
			remaining := int(inPipe)
			for remaining > 0 {
				burst := limiter.Burst()
				if burst <= 0 {
					return written, true, io.ErrShortWrite
				}
				n := remaining
				if n > burst {
					n = burst
				}
				if err := limiter.WaitN(ctx, n); err != nil {
					return written, true, err
				}
				remaining -= n
			}
		}

		// Do not read another source chunk until every byte in the pipe has
		// reached the destination. This makes handoff and backpressure safe.
		for inPipe > 0 {
			var nWritten int64
			var destinationErr error

			err := dstRaw.Write(func(fd uintptr) bool {
				for {
					n, spliceErr := unix.Splice(pipeFD[0], nil, int(fd), nil, int(inPipe), unix.SPLICE_F_NONBLOCK)
					if spliceErr == unix.EINTR {
						continue
					}
					if spliceErr == unix.EAGAIN || spliceErr == unix.EWOULDBLOCK {
						return false
					}
					if spliceErr != nil {
						destinationErr = spliceErr
						return true
					}
					if n == 0 {
						destinationErr = io.ErrShortWrite
						return true
					}
					nWritten = n
					return true
				}
			})
			if err != nil {
				return written, true, err
			}
			if destinationErr != nil {
				return written, true, destinationErr
			}
			inPipe -= nWritten
			written += nWritten
		}
	}
}

func rawSpliceChunkSize(limiter *rate.Limiter) int {
	if limiter == nil {
		return maxRawSpliceChunk
	}
	limit := float64(limiter.Limit())
	if math.IsNaN(limit) || limit <= 0 {
		return 1 << 10
	}
	if math.IsInf(limit, 1) || limit/200 >= maxRawSpliceChunk {
		return maxRawSpliceChunk
	}
	chunk := int(limit / 200) // about 5ms of the configured rate
	if chunk < 1<<10 {
		chunk = 1 << 10
	}
	if chunk > maxRawSpliceChunk {
		chunk = maxRawSpliceChunk
	}
	if burst := limiter.Burst(); burst > 0 && chunk > burst {
		chunk = burst
	}
	return chunk
}
