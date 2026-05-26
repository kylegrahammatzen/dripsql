// Span carries a named phase budget with wall time and row counters and is nil-receiver safe.
// WriteSegment returns a per-call tree that the engine roots under a statement span.
package storage

import (
	"fmt"
	"strings"
	"time"
)

type Span struct {
	Name     string
	Wall     time.Duration
	Rows     int64
	Bytes    int64
	Calls    int
	Children []*Span

	start time.Time
}

func NewSpan(name string) *Span {
	return &Span{Name: name, start: time.Now()}
}

func (s *Span) Child(name string) *Span {
	if s == nil {
		return nil
	}
	c := NewSpan(name)
	s.Children = append(s.Children, c)
	return c
}

func (s *Span) AppendChild(child *Span) {
	if s == nil || child == nil {
		return
	}
	s.Children = append(s.Children, child)
}

func (s *Span) End() {
	if s == nil {
		return
	}
	s.Wall = time.Since(s.start)
	s.Calls++
}

func (s *Span) AddRows(n int64) {
	if s != nil {
		s.Rows += n
	}
}
func (s *Span) AddBytes(n int64) {
	if s != nil {
		s.Bytes += n
	}
}

func (s *Span) Tree() string {
	if s == nil {
		return ""
	}
	var sb strings.Builder
	s.write(&sb, 0)
	return sb.String()
}

func (s *Span) write(sb *strings.Builder, depth int) {
	indent := strings.Repeat("  ", depth)
	fmt.Fprintf(sb, "%s%-22s wall=%s", indent, s.Name, s.Wall)
	if s.Rows > 0 {
		fmt.Fprintf(sb, " rows=%d", s.Rows)
	}
	if s.Bytes > 0 {
		fmt.Fprintf(sb, " bytes=%d", s.Bytes)
	}
	if s.Calls > 1 {
		fmt.Fprintf(sb, " calls=%d", s.Calls)
	}
	sb.WriteByte('\n')
	for _, c := range s.Children {
		c.write(sb, depth+1)
	}
}
