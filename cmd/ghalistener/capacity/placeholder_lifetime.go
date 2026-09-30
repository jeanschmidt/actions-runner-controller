package capacity

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// placeholderLifetime returns a lifetime from 1.5x to just under 2x timeout,
// up to integer truncation, derived from key: equal keys get equal
// lifetimes, and pairs created in one burst do not all expire at once.
func placeholderLifetime(timeout time.Duration, key string) time.Duration {
	lifetime := timeout * 3 / 2
	if spread := timeout / 2; spread > 0 {
		sum := sha256.Sum256([]byte(key))
		lifetime += time.Duration(binary.BigEndian.Uint64(sum[:8]) % uint64(spread))
	}
	return lifetime
}
