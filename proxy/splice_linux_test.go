//go:build linux || android

package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	stdnet "net"
	"testing"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

func newRawSpliceTCPPair(t *testing.T) (*stdnet.TCPConn, *stdnet.TCPConn) {
	t.Helper()
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan *stdnet.TCPConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn.(*stdnet.TCPConn)
	}()

	clientConn, err := stdnet.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case serverConn := <-accepted:
		return clientConn.(*stdnet.TCPConn), serverConn
	case err := <-acceptErr:
		clientConn.Close()
		t.Fatal(err)
	}
	return nil, nil
}

func runRawSpliceTest(t *testing.T, payload []byte, limiter *rate.Limiter, slowRead bool) (time.Duration, []byte) {
	t.Helper()
	sourceClient, sourceServer := newRawSpliceTCPPair(t)
	destClient, destServer := newRawSpliceTCPPair(t)
	defer sourceClient.Close()
	defer sourceServer.Close()
	defer destClient.Close()
	defer destServer.Close()

	writeDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(sourceClient, bytes.NewReader(payload))
		if closeErr := sourceClient.CloseWrite(); err == nil {
			err = closeErr
		}
		writeDone <- err
	}()

	readDone := make(chan []byte, 1)
	go func() {
		var out bytes.Buffer
		buf := make([]byte, 8*1024)
		for {
			n, err := destClient.Read(buf)
			if n > 0 {
				_, _ = out.Write(buf[:n])
				if slowRead {
					time.Sleep(100 * time.Microsecond)
				}
			}
			if err != nil {
				if err != io.EOF {
					t.Errorf("destination read: %v", err)
				}
				break
			}
		}
		readDone <- out.Bytes()
	}()

	start := time.Now()
	written, handled, err := copyRawConnWithSplice(context.Background(), sourceServer, destServer, limiter, nil, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("copyRawConnWithSplice: %v", err)
	}
	if !handled {
		t.Fatal("expected splice path to handle TCP sockets")
	}
	if written != int64(len(payload)) {
		t.Fatalf("written = %d, want %d", written, len(payload))
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("source write: %v", err)
	}
	if err := destServer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	return elapsed, <-readDone
}

func TestCopyRawConnWithSplicePreservesBytes(t *testing.T) {
	payload := make([]byte, 4<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	_, received := runRawSpliceTest(t, payload, nil, false)
	if !bytes.Equal(received, payload) {
		t.Fatalf("payload mismatch: received %d bytes, want %d", len(received), len(payload))
	}
}

func TestCopyRawConnWithSpliceAppliesSharedLimiter(t *testing.T) {
	payload := make([]byte, 2<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	limiter := rate.NewLimiter(rate.Limit(1<<20), 1<<20)
	elapsed, received := runRawSpliceTest(t, payload, limiter, false)
	if !bytes.Equal(received, payload) {
		t.Fatalf("payload mismatch: received %d bytes, want %d", len(received), len(payload))
	}
	if elapsed < 700*time.Millisecond {
		t.Fatalf("rate limiter was bypassed: 2 MiB at 1 MiB/s completed in %s", elapsed)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("unexpectedly slow splice rate limiting: %s", elapsed)
	}
}

func TestCopyRawConnWithSpliceHandlesBackpressure(t *testing.T) {
	payload := make([]byte, 4<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	_, received := runRawSpliceTest(t, payload, nil, true)
	if !bytes.Equal(received, payload) {
		t.Fatalf("payload mismatch under backpressure: received %d bytes, want %d", len(received), len(payload))
	}
}

func TestCopyRawConnWithSpliceSharesLimiterAcrossStreams(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1<<20), 1<<20)
	payloads := [2][]byte{make([]byte, 1<<20), make([]byte, 1<<20)}
	for i := range payloads {
		if _, err := rand.Read(payloads[i]); err != nil {
			t.Fatal(err)
		}
	}

	type stream struct {
		sourceClient *stdnet.TCPConn
		sourceServer *stdnet.TCPConn
		destClient   *stdnet.TCPConn
		destServer   *stdnet.TCPConn
	}
	streams := [2]stream{}
	for i := range streams {
		streams[i].sourceClient, streams[i].sourceServer = newRawSpliceTCPPair(t)
		streams[i].destClient, streams[i].destServer = newRawSpliceTCPPair(t)
		defer streams[i].sourceClient.Close()
		defer streams[i].sourceServer.Close()
		defer streams[i].destClient.Close()
		defer streams[i].destServer.Close()
	}

	writeErrors := make(chan error, len(streams))
	readResults := make(chan struct { index int; data []byte }, len(streams))
	for i := range streams {
		s := streams[i]
		payload := payloads[i]
		go func() {
			_, err := io.Copy(s.sourceClient, bytes.NewReader(payload))
			if closeErr := s.sourceClient.CloseWrite(); err == nil {
				err = closeErr
			}
			writeErrors <- err
		}()
		go func(index int) {
			data, err := io.ReadAll(s.destClient)
			if err != nil {
				t.Errorf("destination read: %v", err)
			}
			readResults <- struct {
				index int
				data  []byte
			}{index: index, data: data}
		}(i)
	}

	start := time.Now()
	var wg sync.WaitGroup
	copyErrors := make(chan error, len(streams))
	for i := range streams {
		wg.Add(1)
		s := streams[i]
		go func() {
			defer wg.Done()
			written, handled, err := copyRawConnWithSplice(context.Background(), s.sourceServer, s.destServer, limiter, nil, nil)
			if err == nil && !handled {
				err = io.ErrUnexpectedEOF
			}
			if err == nil && written != 1<<20 {
				err = io.ErrShortWrite
			}
			if err == nil {
				err = s.destServer.CloseWrite()
			}
			copyErrors <- err
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	for range streams {
		if err := <-copyErrors; err != nil {
			t.Fatal(err)
		}
		if err := <-writeErrors; err != nil {
			t.Fatal(err)
		}
	}
	for range streams {
		result := <-readResults
		if !bytes.Equal(result.data, payloads[result.index]) {
			t.Fatalf("stream %d payload mismatch: received %d bytes, want %d", result.index, len(result.data), len(payloads[result.index]))
		}
	}
	if elapsed < 700*time.Millisecond {
		t.Fatalf("two streams bypassed their shared limiter: 2 MiB completed in %s", elapsed)
	}
}
