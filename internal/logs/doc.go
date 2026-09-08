// Package logs implements Drain log-template extraction and the template
// dictionary that backs tracelens.logs.template_id.
//
// See drain.go for the tree structure and algorithm, and store.go for how
// dense ids are allocated and persisted.
//
// Known, accepted limitation: template_id is stable only within one
// assembler process's lifetime. Coordinating one dense id space across
// multiple replicas needs a shared sequence allocator (etcd/ZooKeeper-style),
// which is new infrastructure nothing else in this project uses, and the
// actually-deployed topology today is a single assembler replica. The same
// template text can therefore get a different id after a process restart, or
// on a second replica if one is ever added. Documented here rather than
// hidden, the same way phase 1 documented its own accepted gaps.
package logs
