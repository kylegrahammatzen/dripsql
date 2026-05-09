package storage

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (s *Store) AppendBuffered(ctx context.Context, table catalog.TableDef, batch vector.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()

	state, err := s.appendTableState(table)
	if err != nil {
		return err
	}
	state.bufferMu.Lock()
	defer state.bufferMu.Unlock()
	if state.publishErr != nil {
		return state.publishErr
	}
	if state.buffer == nil {
		state.buffer, err = s.NewIngestBuffer(table, IngestBufferOptions{})
		if err != nil {
			return err
		}
	}
	runs, err := state.buffer.AppendSealed(ctx, batch)
	if err != nil {
		return err
	}
	s.enqueueBufferedRunsLocked(table, state, runs)
	return nil
}

func (s *Store) FlushBuffered(ctx context.Context, table catalog.TableDef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table)
	if err != nil {
		return err
	}
	return s.flushTableBuffered(ctx, state)
}

func (s *Store) FlushAllBuffered(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()
	return s.flushAllBuffered(ctx)
}

func (s *Store) flushAllBuffered(ctx context.Context) error {
	states := s.tableStatesSnapshot()
	for _, state := range states {
		if err := s.flushTableBuffered(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) tableStatesSnapshot() []*tableState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	states := make([]*tableState, 0, len(s.tables))
	for _, state := range s.tables {
		states = append(states, state)
	}
	return states
}

func (s *Store) flushTableBuffered(ctx context.Context, state *tableState) error {
	state.bufferMu.Lock()
	defer state.bufferMu.Unlock()
	if state.publishErr != nil {
		return state.publishErr
	}
	for state.publishing {
		state.bufferCond.Wait()
	}
	if state.publishErr != nil {
		return state.publishErr
	}
	if state.buffer != nil {
		run, ok, err := state.buffer.FlushSealed(ctx)
		if err != nil {
			return err
		}
		if ok {
			if err := s.publishBufferedRun(ctx, state.table, state, run); err != nil {
				state.publishErr = fmt.Errorf("buffered publish failed for table %d: %w", state.table.ID, err)
				return state.publishErr
			}
			state.buffer.RecycleRun(run)
		}
	}
	if state.publishErr != nil {
		return state.publishErr
	}
	return ctx.Err()
}

func (s *Store) enqueueBufferedRunsLocked(table catalog.TableDef, state *tableState, runs []SealedRun) {
	for _, run := range runs {
		if len(run.Batches) != 0 {
			state.pendingRuns = append(state.pendingRuns, run)
		}
	}
	if len(state.pendingRuns) == 0 || state.publishing {
		return
	}
	state.publishing = true
	go s.runBufferedPublisher(table, state)
}

func (s *Store) runBufferedPublisher(table catalog.TableDef, state *tableState) {
	for {
		state.bufferMu.Lock()
		if state.publishErr != nil || len(state.pendingRuns) == 0 {
			state.pendingRuns = nil
			state.publishing = false
			state.bufferCond.Broadcast()
			state.bufferMu.Unlock()
			return
		}
		run := state.pendingRuns[0]
		state.pendingRuns[0] = SealedRun{}
		state.pendingRuns = state.pendingRuns[1:]
		state.bufferMu.Unlock()

		if err := s.publishBufferedRun(context.Background(), table, state, run); err != nil {
			state.bufferMu.Lock()
			if state.publishErr == nil {
				state.publishErr = fmt.Errorf("background buffered publish failed for table %d: %w", table.ID, err)
			}
			state.pendingRuns = nil
			state.publishing = false
			state.bufferCond.Broadcast()
			state.bufferMu.Unlock()
			return
		}
		state.bufferMu.Lock()
		if state.buffer != nil {
			state.buffer.RecycleRun(run)
		}
		state.bufferMu.Unlock()
	}
}

func (s *Store) publishBufferedRun(ctx context.Context, table catalog.TableDef, state *tableState, run SealedRun) error {
	if len(run.Batches) == 0 {
		return nil
	}
	state.appendMu.Lock()
	defer state.appendMu.Unlock()
	_, err := s.appendBatchesToState(ctx, table, state, run.Batches)
	return err
}
