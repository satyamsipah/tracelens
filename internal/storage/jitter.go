package storage

import (
	"math/rand/v2"
	"time"
)

// jitter returns a duration uniformly distributed in [d/2, d).
//
// "Full jitter" in the AWS sense. The point is decorrelation: without it,
// every assembler replica that failed at the same instant retries at the same
// instant, which re-creates the overload it is backing off from.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
