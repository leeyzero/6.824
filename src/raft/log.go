package raft

import (
	"errors"
	"fmt"
	"sync"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Log struct {
	startIndex  int
	startTerm   int
	entries     []*LogEntry
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	mu          sync.Mutex
}

func NewLog(startIndex int, startTerm int, commitIndex int, lastApplied int, entries []*LogEntry, applyCh chan ApplyMsg) *Log {
	if entries == nil {
		entries = make([]*LogEntry, 0)
	}

	return &Log{
		startIndex:  startIndex,
		startTerm:   startTerm,
		entries:     entries,
		commitIndex: 0, // TODO: initialize from persist
		lastApplied: 0, // TODO: initialize from persist
		applyCh:     applyCh,
	}
}

func (l *Log) CreateEntry(term int, cmd interface{}) *LogEntry {
	return &LogEntry{
		Index:   l.nextIndex(),
		Term:    term,
		Command: cmd,
	}
}

func (l *Log) CommitIndex() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commitIndex
}

// LastInfo return last log index and term
func (l *Log) LastInfo() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 {
		return l.startIndex, l.startTerm
	}

	lastEntry := l.entries[len(l.entries)-1]
	return lastEntry.Index, lastEntry.Term
}

// AppendEntry ...
func (l *Log) AppendEntry(entry *LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term < lastEntry.Term {
			return fmt.Errorf("raft.log: cannot append entry with earlier term")
		} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
			return fmt.Errorf("raft.log: cannot append entry with earlier idnex")
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

// Truncate ...
func (l *Log) Truncate(index int, term int) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Do not allow committed entries to be truncated.
	if index < l.commitIndex {
		return fmt.Errorf("index already committed")
	}

	return errors.New("NOT IMPLEMENTED")
}

// Sync ...
func (l *Log) Sync() {
	// TODO:
}

func (l *Log) SetCommitIndex(index int) {
}

// CommintIndex ...
func (l *Log) GetCommitIndex(index int) error {
	return errors.New("NOT IMPLEMENTED")
}

func (l *Log) nextIndex() int {
	return 0
}
