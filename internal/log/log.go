// Package log provides the Raft append-only log abstraction.
//
// The log is 1-indexed, matching the Raft paper. Index 0 is reserved as a
// sentinel (it never holds a real entry). All methods that accept or return
// indices use 1-based indexing.
//
// The log is NOT thread-safe. The caller (Raft) is responsible for holding
// its mutex whenever the log is accessed.
//
// This file is provided. Do not modify it.
package log

import "fmt"

// LogEntry is a single entry in the Raft log.
type LogEntry struct {
	Index   int64  // 1-indexed position in the log
	Term    int64  // the term in which this entry was created
	Command string // encoded application command (e.g. "put:key:value")
}

// Log is an append-only Raft log, 1-indexed.
type Log struct {
	entries []LogEntry // entries[0] is the sentinel (Index=0, Term=0)
}

// New returns an empty Log with a sentinel entry at index 0.
func New() *Log {
	return &Log{
		entries: []LogEntry{{Index: 0, Term: 0, Command: ""}},
	}
}

// Append adds entry to the end of the log.
// Panics if entry.Index != LastIndex()+1 (caller must set index correctly).
func (l *Log) Append(entry LogEntry) {
	expected := l.LastIndex() + 1
	if entry.Index != expected {
		panic(fmt.Sprintf("log: Append index %d != expected %d", entry.Index, expected))
	}
	l.entries = append(l.entries, entry)
}

// At returns the entry at the given 1-based index and true, or the zero
// LogEntry and false if the index is out of range.
func (l *Log) At(index int64) (LogEntry, bool) {
	if index <= 0 || index >= int64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// LastIndex returns the index of the last entry, or 0 if the log is empty
// (only the sentinel exists).
func (l *Log) LastIndex() int64 {
	return int64(len(l.entries)) - 1
}

// LastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *Log) LastTerm() int64 {
	return l.entries[len(l.entries)-1].Term
}

// Truncate removes all entries with index >= fromIndex.
// If fromIndex <= 0 or > LastIndex(), this is a no-op.
func (l *Log) Truncate(fromIndex int64) {
	if fromIndex <= 0 || fromIndex >= int64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:fromIndex]
}

// Entries returns all entries in the half-open range [from, to).
// Indices are 1-based. Returns an empty slice if the range is invalid or empty.
func (l *Log) Entries(from, to int64) []LogEntry {
	n := int64(len(l.entries))
	if from <= 0 {
		from = 1
	}
	if to > n {
		to = n
	}
	if from >= to {
		return nil
	}
	result := make([]LogEntry, to-from)
	copy(result, l.entries[from:to])
	return result
}

// Len returns the number of real entries (not counting the sentinel).
func (l *Log) Len() int64 {
	return int64(len(l.entries)) - 1
}
