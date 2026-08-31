package distsync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MouXiaoJun/distsync/internal/lua"
	"github.com/redis/go-redis/v9"
)

// Only containers created here can be stopped/restarted. Never accepts a server
// address or container ID supplied by the environment. Normal tests still use
// miniredis unless explicitly pointed at a disposable integration server.
type faultServer struct {
	t    *testing.T
	id   string
	addr string
}

func dockerCommand(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("docker %v: %w: %s", args, err, stderr.String())
	}
	return strings.TrimSpace(string(out)), nil
}

func newFaultServer(t *testing.T) *faultServer {
	t.Helper()
	image := os.Getenv("DISTSYNC_FAULT_IMAGE")
	if image == "" {
		t.Skip("set DISTSYNC_FAULT_IMAGE=redis:7-alpine or valkey/valkey:8 for isolated Docker fault tests")
	}
	// Reserve a loopback port, then give that explicit port to Docker so a
	// restart keeps the address. A bind race fails startup, never reuses a service.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	command := "redis-server"
	if strings.Contains(image, "valkey") {
		command = "valkey-server"
	}
	id, err := dockerCommand("create", "--label", "distsync.test=faults", "--publish", addr+":6379",
		image, command, "--save", "", "--appendonly", "no")
	if err != nil {
		t.Fatal(err)
	}
	s := &faultServer{t: t, id: id, addr: addr}
	t.Cleanup(func() {
		if _, err := dockerCommand("rm", "--force", "--volumes", id); err != nil {
			t.Error(err)
		} else {
			t.Logf("removed own fault container %s and its disposable volumes (%s)", id[:12], addr)
		}
	})
	s.start()
	rdb := s.client(nil)
	info, err := rdb.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.Contains(line, "version:") {
			t.Log(strings.TrimSpace(line))
		}
	}
	t.Logf("own fault container %s image=%s loopback=%s", id[:12], image, addr)
	return s
}

func (s *faultServer) start() {
	s.t.Helper()
	if _, err := dockerCommand("start", s.id); err != nil {
		s.t.Fatal(err)
	}
	rdb := s.client(nil)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			return
		} else if time.Now().After(deadline) {
			s.t.Fatalf("own server did not start: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *faultServer) kill() {
	s.t.Helper()
	if _, err := dockerCommand("kill", "--signal", "KILL", s.id); err != nil {
		s.t.Fatal(err)
	}
}

func (s *faultServer) client(wire *responseFault) *redis.Client {
	opts := &redis.Options{Addr: s.addr, Protocol: 2, DisableIdentity: true,
		MaxRetries: -1, DialTimeout: 150 * time.Millisecond, ReadTimeout: time.Second,
		WriteTimeout: time.Second, ContextTimeoutEnabled: true}
	if wire != nil {
		opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: opts.DialTimeout}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &responseFaultConn{Conn: conn, fault: wire}, nil
		}
	}
	rdb := redis.NewClient(opts)
	s.t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// The server really executes the command and returns bytes. Hold those bytes
// before go-redis can parse them, optionally replacing them with a broken socket.
// This is a controlled response-path fault, not a distributed-system simulator.
type responseFault struct {
	mu      sync.Mutex
	armed   bool
	drop    bool
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func newResponseFault(t *testing.T) *responseFault {
	f := &responseFault{entered: make(chan struct{}), resume: make(chan struct{})}
	t.Cleanup(f.unblock)
	return f
}

func (f *responseFault) arm(drop bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed, f.drop = true, drop
}

func (f *responseFault) unblock() { f.once.Do(func() { close(f.resume) }) }

func (f *responseFault) wait(t *testing.T) {
	t.Helper()
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no response reached the wire fault")
	}
}

type responseFaultConn struct {
	net.Conn
	fault *responseFault
}

func (c *responseFaultConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	f := c.fault
	f.mu.Lock()
	armed, drop := f.armed && n > 0, f.drop
	if armed {
		f.armed = false
	}
	f.mu.Unlock()
	if armed {
		close(f.entered)
		<-f.resume
		if drop {
			_ = c.Close()
			return 0, io.ErrUnexpectedEOF
		}
	}
	return n, err
}

func warmFaultClient(t *testing.T, rdb *redis.Client) {
	t.Helper()
	for _, script := range []*redis.Script{lua.SingleAcquire, lua.SingleRenew, lua.SingleRelease,
		lua.RWWriteLock, lua.RWReadLock, lua.SemAcquire, lua.SemRenew, lua.SemRelease} {
		if err := script.Load(context.Background(), rdb).Err(); err != nil {
			t.Fatal(err)
		}
	}
}
