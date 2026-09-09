// Drain is an online log-template extraction algorithm (He et al., "Drain:
// An Online Log Parsing Approach with Fixed Depth Tree", ICWS 2017), built
// here from scratch rather than pulled from a library.
//
// The tree:
//
//	root
//	 └─ level 1: keyed by TOKEN COUNT            len=5, len=7, len=12, ...
//	     └─ levels 2..depth-1: keyed by LEADING TOKEN, position by position
//	         │  a token that looks numeric is routed to a wildcard child
//	         │  immediately rather than branching on its literal value --
//	         │  branching the tree on what is almost certainly an id/count
//	         │  just fragments it for no benefit
//	         │  a position with more than maxChildren distinct literal
//	         │  values seen also falls back to the wildcard child, capping
//	         │  tree WIDTH the same way depth caps tree HEIGHT
//	         └─ leaf: []*cluster
//
// At a leaf, a new log's tokens are compared against every existing cluster:
// similarity = (positions equal, treating a template's "<*>" as an automatic
// match) / length. The best match at or above the threshold wins: any
// position where the incoming tokens disagree with the current template gets
// generalized to "<*>" (so templates only ever get MORE general over time,
// never less), and the incoming line reuses that cluster's id. No match
// clears the threshold -> a new cluster is created with the next dense id.
package logs

import (
	"strconv"
	"strings"
	"sync"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// wildcard is the sentinel marking a generalized template position.
const wildcard = "<*>"

// Config holds Drain's three configurable knobs, plus the template cap.
type Config struct {
	// Depth is how many leading-token levels to descend before reaching a
	// leaf (the root's length-level counts as level 1). Deeper trims the
	// leaf's candidate-cluster list more aggressively at the cost of being
	// less tolerant of variation in the early tokens.
	Depth int

	// SimilarityThreshold is the minimum fraction of matching positions
	// (wildcards counting as automatic matches) for an incoming log to merge
	// into an existing cluster rather than starting a new one.
	SimilarityThreshold float64

	// MaxChildren bounds how many distinct literal token values one internal
	// node may branch on before further distinct values fall back to a
	// shared wildcard child. Bounds tree WIDTH the way Depth bounds HEIGHT --
	// Drain's own memory footprint must stay bounded too.
	MaxChildren int

	// MaxTemplates caps the total number of distinct clusters held across
	// the whole tree. On breach, the least-recently-matched cluster is
	// evicted (LRU) to make room -- consistent with the eviction discipline
	// used everywhere else in this system (bounded resource, explicit
	// counter, never silent unbounded growth). A future log that would have
	// matched the evicted cluster simply gets a new id instead of losing data.
	MaxTemplates int

	// MaxClustersPerLeaf bounds findOrCreate's linear similarity scan, which
	// is O(clusters at that leaf) per call. MEASURED: a leaf holding 2000
	// clusters costs 138x a leaf holding one (57.5us vs 416ns per Parse
	// call, BenchmarkDrainParseManyClustersAtOneLeaf vs
	// BenchmarkDrainParseSteadyState) -- MaxTemplates alone does not bound
	// this, since global LRU eviction protects a "hot" leaf (touched often)
	// at the expense of other leaves, letting one leaf's list grow toward
	// the entire tree-wide budget. Eviction here is per-leaf LRU, distinct
	// from MaxTemplates' tree-wide eviction; both can fire independently.
	MaxClustersPerLeaf int
}

// DefaultConfig returns sane defaults for a general-purpose log corpus.
func DefaultConfig() Config {
	return Config{
		Depth:               4,
		SimilarityThreshold: 0.6,
		MaxChildren:         100,
		MaxTemplates:        10_000,
		MaxClustersPerLeaf:  200,
	}
}

// node is one position in the tree. Internal nodes have children; leaves
// have clusters. A node is never both.
type node struct {
	children map[string]*node
	clusters []*cluster
}

// cluster is one template candidate at a leaf.
type cluster struct {
	id       uint32
	template []string
	count    uint64
	// lruGen is the tree's generation counter at the cluster's last match,
	// used to find the least-recently-matched cluster on eviction without
	// keeping a separately-maintained ordered list.
	lruGen uint64
}

// Match is the result of parsing one log line: which template it belongs to
// (a stable, dense id) and the tokens that were generalized out as
// variables, in template order.
type Match struct {
	TemplateID uint32
	Template   []string
	Params     []string
	IsNew      bool
	// Evicted is true when creating this cluster required evicting the
	// least-recently-matched cluster elsewhere in the tree to stay under
	// MaxTemplates. Always false when IsNew is false.
	Evicted bool
	// Changed is true when Template differs from what it was before this
	// call -- either IsNew (nothing to compare against yet), or an existing
	// cluster was just generalized further (a position widened to "<*>").
	// A caller persisting templates into a dictionary should upsert on
	// Changed, not just on IsNew: persisting only at creation would leave
	// the dictionary holding the ORIGINAL, less-general literal text forever
	// once a later log causes that template to widen.
	Changed bool
}

// Drain is one parse tree plus its dense id allocator. Not safe for
// concurrent use without external locking; Tree wraps this with a mutex.
type drain struct {
	cfg      Config
	root     *node
	nextID   uint32
	nextGen  uint64
	clusters map[uint32]*cluster // every cluster, by id, for O(1) eviction lookup
}

func newDrain(cfg Config) *drain {
	return &drain{
		cfg:      cfg,
		root:     &node{children: map[string]*node{}},
		clusters: map[uint32]*cluster{},
	}
}

// Parse tokenizes body on whitespace and descends the tree, matching or
// creating a cluster, returning the resulting template/params.
func (d *drain) Parse(body string) Match {
	tokens := strings.Fields(body)
	if len(tokens) == 0 {
		tokens = []string{""}
	}

	leaf := d.descend(tokens)
	cl, isNew, evicted, changed := d.findOrCreate(leaf, tokens)
	cl.count++

	params := extractParams(tokens, cl.template)

	return Match{
		TemplateID: cl.id,
		Template:   append([]string(nil), cl.template...),
		Params:     params,
		IsNew:      isNew,
		Evicted:    evicted,
		Changed:    isNew || changed,
	}
}

// descend walks length, then leading tokens, creating nodes as needed, and
// returns the leaf.
func (d *drain) descend(tokens []string) *node {
	cur := d.root

	lengthKey := strconv.Itoa(len(tokens))
	cur = d.childFor(cur, lengthKey)

	// Level 1 is length; levels 2..depth-1 are leading tokens. depth-2
	// leading-token levels remain (depth counts the length level and the
	// leaf itself), floored at 0 for a very shallow configured depth.
	leadingLevels := d.cfg.Depth - 2
	if leadingLevels < 0 {
		leadingLevels = 0
	}
	if leadingLevels > len(tokens) {
		leadingLevels = len(tokens)
	}

	for i := 0; i < leadingLevels; i++ {
		key := tokens[i]
		if looksNumeric(key) {
			key = wildcard
		} else if len(cur.children) >= d.cfg.MaxChildren {
			if _, exists := cur.children[key]; !exists {
				key = wildcard
			}
		}
		cur = d.childFor(cur, key)
	}
	return cur
}

func (d *drain) childFor(n *node, key string) *node {
	child, ok := n.children[key]
	if !ok {
		child = &node{children: map[string]*node{}}
		n.children[key] = child
	}
	return child
}

// findOrCreate finds the best-matching cluster at leaf, or creates one.
func (d *drain) findOrCreate(leaf *node, tokens []string) (cl *cluster, isNew, evicted, changed bool) {
	var best *cluster
	var bestScore float64

	for _, c := range leaf.clusters {
		score := similarity(tokens, c.template)
		if score > bestScore {
			bestScore, best = score, c
		}
	}

	// touch bumps and stamps the generation counter for whichever cluster
	// this call resolves to -- matched or newly created -- BEFORE any
	// eviction check runs. A brand-new cluster must be stamped here, not
	// after this function returns: its lruGen zero-value would otherwise be
	// the lowest in the tree, making evictLRU immediately evict the cluster
	// this very call just created.
	touch := func(c *cluster) {
		d.nextGen++
		c.lruGen = d.nextGen
	}

	if best != nil && bestScore >= d.cfg.SimilarityThreshold {
		widened := merge(best, tokens)
		touch(best)
		return best, false, false, widened
	}

	// Per-leaf cap, checked BEFORE adding the new cluster: bounds this
	// leaf's own similarity-scan cost (the O(clusters at leaf) loop above),
	// independent of the tree-wide MaxTemplates cap, which alone lets one
	// "hot" leaf (protected by global LRU because it's touched often) grow
	// toward the entire tree's budget while other leaves starve.
	if d.cfg.MaxClustersPerLeaf > 0 && len(leaf.clusters) >= d.cfg.MaxClustersPerLeaf {
		d.evictLeafLRU(leaf)
		evicted = true
	}

	cl = &cluster{
		id:       d.allocateID(),
		template: append([]string(nil), tokens...),
	}
	touch(cl)
	leaf.clusters = append(leaf.clusters, cl)
	d.clusters[cl.id] = cl

	if len(d.clusters) > d.cfg.MaxTemplates {
		d.evictLRU()
		evicted = true
	}
	return cl, true, evicted, true
}

// evictLeafLRU removes the least-recently-matched cluster from THIS leaf
// specifically (and from the tree-wide id index), bounding one leaf's
// cluster list independent of global LRU pressure.
func (d *drain) evictLeafLRU(leaf *node) {
	if len(leaf.clusters) == 0 {
		return
	}
	oldestIdx := 0
	oldestGen := leaf.clusters[0].lruGen
	for i, c := range leaf.clusters[1:] {
		if c.lruGen < oldestGen {
			oldestGen, oldestIdx = c.lruGen, i+1
		}
	}
	evictedID := leaf.clusters[oldestIdx].id
	leaf.clusters = append(leaf.clusters[:oldestIdx], leaf.clusters[oldestIdx+1:]...)
	delete(d.clusters, evictedID)
}

// allocateID hands out the next dense id. Dense and monotonic, NOT a hash --
// the T64 codec on tracelens.logs.template_id relies on ids clustering near a
// small range with mostly-zero high bits, which only a counter provides.
func (d *drain) allocateID() uint32 {
	d.nextID++
	return d.nextID
}

// evictLRU removes the least-recently-matched cluster across the whole tree.
// Returns the evicted id so the caller can count it.
func (d *drain) evictLRU() uint32 {
	var oldest *cluster
	oldestGen := ^uint64(0)
	for _, cl := range d.clusters {
		if cl.lruGen < oldestGen {
			oldestGen, oldest = cl.lruGen, cl
		}
	}
	if oldest == nil {
		return 0
	}
	delete(d.clusters, oldest.id)
	removeClusterFromTree(d.root, oldest.id)
	return oldest.id
}

// removeClusterFromTree walks the whole tree to find and drop the evicted
// cluster from its leaf's slice. Eviction is rare (only at the cap) relative
// to Parse, so a full-tree walk here is the right trade against threading a
// parent pointer through every node just to support an infrequent operation.
func removeClusterFromTree(n *node, id uint32) bool {
	if len(n.clusters) > 0 {
		for i, cl := range n.clusters {
			if cl.id == id {
				n.clusters = append(n.clusters[:i], n.clusters[i+1:]...)
				return true
			}
		}
		return false
	}
	for _, child := range n.children {
		if removeClusterFromTree(child, id) {
			return true
		}
	}
	return false
}

// similarity is Drain's simSeq: the fraction of positions where the incoming
// tokens equal the template, with a template wildcard counting as an
// automatic match. Templates and tokens here are always the same length
// (both descended the identical length-keyed branch), so no bounds check is
// needed on the shorter of the two.
func similarity(tokens, template []string) float64 {
	if len(tokens) != len(template) {
		return 0
	}
	matches := 0
	for i, tok := range tokens {
		if template[i] == wildcard || template[i] == tok {
			matches++
		}
	}
	return float64(matches) / float64(len(tokens))
}

// merge generalizes template in place: any position where tokens disagrees
// becomes a wildcard. Templates only ever become MORE general, never less.
// Returns whether the template actually widened, so a caller persisting
// templates to a dictionary knows whether the stored text is now stale.
func merge(cl *cluster, tokens []string) bool {
	widened := false
	for i, tok := range tokens {
		if cl.template[i] != wildcard && cl.template[i] != tok {
			cl.template[i] = wildcard
			widened = true
		}
	}
	return widened
}

// extractParams returns the tokens at every wildcard position of template,
// in order -- exactly the "variable parts" the schema's params column holds.
func extractParams(tokens, template []string) []string {
	params := make([]string, 0, len(template))
	for i, t := range template {
		if t == wildcard {
			params = append(params, tokens[i])
		}
	}
	return params
}

// looksNumeric is Drain's eager-wildcard heuristic: a token containing any
// digit is treated as a parameter position during tree descent rather than
// branched on literally, since an id/count/timestamp fragment is far more
// likely to be a parameter than genuine template structure, and branching on
// its near-random literal value would fragment the tree for no benefit.
func looksNumeric(token string) bool {
	for _, r := range token {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}

// Tree is the concurrency-safe, externally usable wrapper around drain.
type Tree struct {
	mu sync.Mutex
	d  *drain
	m  *observability.Metrics
}

// NewTree builds a Drain tree with the given configuration.
func NewTree(cfg Config, m *observability.Metrics) *Tree {
	return &Tree{d: newDrain(cfg), m: m}
}

// Parse is safe for concurrent use.
func (t *Tree) Parse(body string) Match {
	t.mu.Lock()
	defer t.mu.Unlock()

	match := t.d.Parse(body)

	if t.m != nil {
		if match.IsNew {
			t.m.TemplatesCreatedTotal.Inc()
		}
		if match.Evicted {
			t.m.TemplatesEvictedTotal.Inc()
		}
		t.m.TemplatesTotal.Set(float64(len(t.d.clusters)))
	}
	return match
}

// TemplateCount reports how many distinct clusters are currently held.
func (t *Tree) TemplateCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.d.clusters)
}
