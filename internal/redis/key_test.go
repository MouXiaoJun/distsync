package redisx

import (
	"strings"
	"testing"
)

func TestKeyNormalization(t *testing.T) {
	cases := []struct{ in, want string }{
		{"order:10001", "{order:10001}"},
		{"{order:10001}", "{order:10001}"},
		{"user:{id}:cart", "user:{id}:cart"}, // already hash-tagged, left alone
		{"plain", "{plain}"},
		{"", "{}"}, // degenerate but harmless
	}
	for _, c := range cases {
		if got := Key(c.in); got != c.want {
			t.Errorf("Key(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDerivedKeysShareClusterSlot(t *testing.T) {
	// Every primitive's full key family must land on a single Redis Cluster
	// slot, otherwise multi-key Lua scripts fail with CROSSSLOT.
	key := Key("order:10001")
	families := [][]string{
		// Mutex / Leader.
		{key, Derived(key, "fencing")},
		// RWMutex.
		{key, Derived(key, "writer"), Derived(key, "readers"),
			Derived(key, "fencing"), Derived(key, "writer-waiting")},
		// Semaphore / RateLimiter.
		{key, Derived(key, "bucket")},
	}
	for _, fam := range families {
		slot := slotOf(fam[0])
		for _, k := range fam[1:] {
			if got := slotOf(k); got != slot {
				t.Errorf("keys %v must share one slot: %q is on %d, %q on %d",
					fam, fam[0], slot, k, got)
			}
		}
	}
}

// slotOf computes the Redis Cluster slot exactly like Redis does: CRC16 over
// the hash-tag content (between the first '{' and the following '}'), modulo
// 16384. When there is no tag, the whole key is hashed.
func slotOf(key string) uint16 {
	tag := key
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j >= 0 {
			tag = key[i+1 : i+1+j]
		}
	}
	return crc16(tag) % 16384
}

// crc16 is CRC-16/XMODEM, the polynomial Redis uses for slot hashing.
func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
