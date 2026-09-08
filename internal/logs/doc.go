// Package logs will hold Drain log templating (phase 2).
//
// Empty by design in phase 1. The storage schema already reserves its output:
// tracelens.logs has template_id and params columns, written as 0 and empty
// until this lands.
//
// One constraint is already binding. template_id is declared
// CODEC(T64, ZSTD(1)), and T64 works by cropping provably-unused high bits --
// so the id MUST be a DENSE dictionary id assigned from a counter, not a hash
// of the template. A hash has uniformly random high bits, T64 would compress
// nothing, and the column would silently cost 4 bytes per row forever.
//
// That also means the template dictionary is shared state with its own
// lifecycle: it has to survive restarts and be consistent across assembler
// instances, or the same template gets different ids on different nodes.
package logs
