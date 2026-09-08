package sampling

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

// Router implements rendezvous hashing (highest random weight, HRW) over a
// fixed set of replica identities.
//
// This is deliberately NOT what Kafka partitioning does. Kafka partition-key
// hashing is hash(key) % N -- sharding, not consistent hashing -- which is
// why phase 1 fixed the partition count forever: changing N remaps every key.
// Router exists to demonstrate and provide the genuine article: adding or
// removing one replica remaps only ~1/(new N) of keys, leaving the rest of
// the mapping untouched. See docs/DECISIONS.md for why this is a standalone,
// tested primitive rather than the live partition-assignment mechanism today.
type Router struct {
	replicas []string
}

// NewRouter builds a router over a fixed replica set. The input is copied and
// sorted so that two routers built from the same set in different orders are
// identical -- Owner must not depend on slice order.
func NewRouter(replicas []string) *Router {
	cp := make([]string, len(replicas))
	copy(cp, replicas)
	sort.Strings(cp)
	return &Router{replicas: cp}
}

// Owner returns which replica owns key, by computing a hash of (key, replica)
// for every replica and returning the replica with the highest score.
//
// This is O(len(replicas)) per call, which is fine: replica counts are small
// (single digits to low tens), and this runs once per trace decision, not
// once per span.
func (r *Router) Owner(key []byte) string {
	best := r.replicas[0]
	bestScore := hrwScore(key, best)

	// On a tie (astronomically unlikely with a 64-bit hash, but must still be
	// deterministic), the strict ">" below means the first replica scanned
	// keeps it -- and since r.replicas is sorted, "first scanned" is always
	// the lexicographically earliest tied replica.
	for _, replica := range r.replicas[1:] {
		if score := hrwScore(key, replica); score > bestScore {
			bestScore, best = score, replica
		}
	}
	return best
}

// Replicas returns the configured replica set, sorted.
func (r *Router) Replicas() []string {
	out := make([]string, len(r.replicas))
	copy(out, r.replicas)
	return out
}

// hrwScore combines key and replica into one well-mixed 64-bit score.
//
// MEASURED BUG, fixed here: an earlier version fed key+separator+replica
// through one FNV-1a stream. FNV-1a has weak avalanche on short, near-identical
// tails, and replica identities are exactly that ("a","b","c","d" differ by a
// single bit in their last byte) -- TestRouterSpread caught one replica
// winning ~2x its fair share as a result. Hashing key and replica
// INDEPENDENTLY and combining through a strong finalizer (SplitMix64's, a
// well-known high-quality 64-bit bit mixer) means the weak tail-sensitivity of
// hashing a short replica name no longer matters: the finalizer fully
// avalanches the combination regardless.
func hrwScore(key []byte, replica string) uint64 {
	kh := fnv.New64a()
	_, _ = kh.Write(key)

	rh := fnv.New64a()
	_, _ = rh.Write([]byte(replica))

	return splitmix64(kh.Sum64() ^ (rh.Sum64() + 0x9E3779B97F4A7C15))
}

// splitmix64 is the finalizer from the SplitMix64 PRNG: three
// multiply-xor-shift rounds that turn any input into a fully avalanched
// 64-bit output, regardless of how correlated the input bits are.
func splitmix64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// TraceKey converts a 16-byte trace id into the []byte form Owner expects.
func TraceKey(traceID [16]byte) []byte { return traceID[:] }

// PartitionKey converts a Kafka partition id into the []byte form Owner
// expects, for the (currently unused-in-production, but tested) case of
// routing by partition rather than by trace_id directly.
func PartitionKey(partition int32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(partition))
	return buf[:]
}
