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

const maxRawSpliceChunk = 256 << 10

// copyRawConnWithSplice forwards a raw stream through a kernel pipe. When a
// limiter is present, it charges each chunk before the pipe is drained to the
// destination. This keeps the zero-copy splice path while enforcing the same
// shared per-user token bucket used by RateLimitWriter.
func copyRawConnWithSplice(
	ctx context.Context,
	readerConn net.Conn,
	writerConn net.Conn,
	limiter *rate.Limiter,
	onActivity func(),
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

	// Size the pipe for the current chunk, and grow it only when a live
	// bandwidth-limit increase requires larger chunks. The default pipe size
	// remains a safe fallback if the kernel refuses the resize.
	chunkSize := rawSpliceChunkSize(limiter)
	pipeSize := chunkSize
	if actualSize, resizeErr := unix.FcntlInt(uintptr(pipeFD[1]), unix.F_SETPIPE_SZ, chunkSize); resizeErr == nil {
		pipeSize = actualSize
	}

	for {
		// A removed limit is represented by MaxInt burst in Xboard. The pipe
		// is empty at this point, so the caller can safely hand the socket back
		// to Go's original TCPConn.ReadFrom implementation.
		if limiter != nil && limiter.Burst() == int(^uint(0)>>1) {
			return written, false, nil
		}
		// Recalculate the chunk size so live limit changes can also increase
		// the splice chunk and avoid permanently paying small-chunk overhead.
		chunkSize = rawSpliceChunkSize(limiter)
		if chunkSize > pipeSize {
			if actualSize, resizeErr := unix.FcntlInt(uintptr(pipeFD[1]), unix.F_SETPIPE_SZ, chunkSize); resizeErr == nil {
				pipeSize = actualSize
			} else {
				pipeSize = chunkSize
			}
		}
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

		if onActivity != nil {
			onActivity()
		}

		if limiter != nil && limiter.Burst() != int(^uint(0)>>1) {
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
					// A live limit update can shrink Burst between the
					// snapshot above and WaitN's internal check. Re-split
					// against the new burst instead of dropping the stream.
					if ctx.Err() == nil && n > limiter.Burst() {
						continue
					}
					return written, true, err
				}
				remaining -= n
			}
		}

		// Do not read another source chunk until every byte in the pipe has
		// reached the destination. Keep the destination write lock while
		// pumping partial writes, just like Go's standard splice path.
		var destinationErr error
		err = dstRaw.Write(func(fd uintptr) bool {
			for inPipe > 0 {
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
				inPipe -= n
				written += n
			}
			return true
		})
		if err != nil {
			return written, true, err
		}
		if destinationErr != nil {
			return written, true, destinationErr
		}
		if onActivity != nil {
			onActivity()
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
