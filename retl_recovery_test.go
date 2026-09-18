package cron

import (
	"testing"
	"time"
)

// fakeSchedule is a minimal Schedule whose "natural next" tick is fixed, so
// tests can control exactly what ScheduleFirst sees without depending on a
// real cron spec parser. NextWithAfter mimics a real schedule: once the
// search start (whichever of from/after is later) has passed the natural
// tick, it rolls forward to the following occurrence (fixed here at +24h for
// simplicity, standing in for "the next matching instant").
type fakeSchedule struct {
	next time.Time
}

func (s fakeSchedule) Next(from time.Time) time.Time {
	return s.next
}

func (s fakeSchedule) NextWithAfter(from, after time.Time) time.Time {
	if !after.IsZero() && after.After(from) {
		from = after
	}
	if s.next.After(from) {
		return s.next
	}
	return s.next.Add(24 * time.Hour)
}

func withRecoveryTimeLimit(t *testing.T, d time.Duration) {
	t.Helper()
	old := recoveryTimeLimit
	recoveryTimeLimit = d
	t.Cleanup(func() { recoveryTimeLimit = old })
}

// withRecoveryObserver installs fn as RecoveryObserver for the duration of
// the test and restores whatever was there before on cleanup.
func withRecoveryObserver(t *testing.T, fn func(refID string, recovered bool, naturalNext, now time.Time)) {
	t.Helper()
	old := RecoveryObserver
	RecoveryObserver = fn
	t.Cleanup(func() { RecoveryObserver = old })
}

// A job's natural next tick passed recently (within recoveryTimeLimit) by the
// time it's (re-)registered - e.g. right after a process restart. With
// recovery enabled, it should be scheduled to fire almost immediately instead
// of silently rolling forward to its next occurrence.
func TestScheduleFirstRecoversRecentMiss(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)

	now := time.Now()
	naturalNext := now.Add(-5 * time.Minute) // due 5 minutes ago
	prev := naturalNext.Add(-24 * time.Hour) // last run was one occurrence before that

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev}

	// Fires after recoveryFireDelay, plus a per-entry jitter offset that
	// spreads a mass recovery out rather than firing everything at once.
	got := entry.ScheduleFirst(now)
	earliest := now.Add(recoveryFireDelay)
	latest := now.Add(recoveryFireDelay + recoveryFireJitter)
	if got.Before(earliest) || !got.Before(latest) {
		t.Errorf("ScheduleFirst() = %v, want within [%v, %v)", got, earliest, latest)
	}
}

// A restart that recovers many entries at once must not schedule them all for
// the same instant - each entry's fire time is offset by a jitter derived from
// its own identity.
func TestRecoveryJitterSpreadsEntries(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)

	now := time.Now()
	naturalNext := now.Add(-5 * time.Minute)
	prev := naturalNext.Add(-24 * time.Hour)

	fireTimes := make(map[time.Time]int)
	for _, refID := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev, RefID: refID}
		fireTimes[entry.ScheduleFirst(now)]++
	}

	// Not asserting all 8 are unique (hash collisions are legal), just that
	// they don't all collapse onto one instant the way a fixed delay would.
	if len(fireTimes) < 2 {
		t.Errorf("all %d entries scheduled for the same instant, want them spread", len(fireTimes))
	}
}

// Jitter must be stable for a given entry, so a job doesn't drift to a
// different offset on every restart.
func TestRecoveryJitterIsStableForSameRefID(t *testing.T) {
	a := Entry{RefID: "retl:src-1:sub-1"}.recoveryJitter()
	b := Entry{RefID: "retl:src-1:sub-1"}.recoveryJitter()
	if a != b {
		t.Errorf("recoveryJitter() = %v then %v, want stable for the same RefID", a, b)
	}
	if a < 0 || a >= recoveryFireJitter {
		t.Errorf("recoveryJitter() = %v, want within [0, %v)", a, recoveryFireJitter)
	}
}

// RecoveryObserver is caller-supplied but runs on the scheduler's goroutine,
// which nothing else recovers - a panic in it must not escape and take the
// whole scheduler down with it.
func TestRecoveryObserverPanicDoesNotEscape(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)
	withRecoveryObserver(t, func(refID string, recovered bool, naturalNext, now time.Time) {
		panic("observer blew up")
	})

	now := time.Now()
	naturalNext := now.Add(-5 * time.Minute)
	prev := naturalNext.Add(-24 * time.Hour)

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev, RefID: "retl:src-1:sub-1"}

	// Must still return a recovered fire time - the panic is contained, not
	// allowed to abort the scheduling decision.
	got := entry.ScheduleFirst(now)
	if !got.After(now) {
		t.Errorf("ScheduleFirst() = %v, want a future fire time despite the panicking observer", got)
	}
}

// A miss older than recoveryTimeLimit is left to the original behavior -
// rolled forward to its next occurrence, not fired immediately - even with
// recovery enabled. This is the same "left to schedule forward normally"
// principle job_store.go's own CatchUpWindow already applies.
func TestScheduleFirstLeavesOldMissesAlone(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)

	now := time.Now()
	naturalNext := now.Add(-30 * time.Minute) // due 30 minutes ago - beyond the limit
	prev := naturalNext.Add(-24 * time.Hour)

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev}

	got := entry.ScheduleFirst(now)
	want := naturalNext.Add(24 * time.Hour) // rolled forward, untouched
	if !got.Equal(want) {
		t.Errorf("ScheduleFirst() = %v, want %v (rolled forward, untouched)", got, want)
	}
}

// With recovery disabled (the default, zero value), ScheduleFirst's behavior
// is byte-for-byte unchanged from before this change, for any consumer that
// doesn't opt in via RECOVERY_TIME_LIMIT.
func TestScheduleFirstDisabledByDefault(t *testing.T) {
	withRecoveryTimeLimit(t, 0)

	now := time.Now()
	naturalNext := now.Add(-1 * time.Minute) // due 1 minute ago - well within any reasonable limit
	prev := naturalNext.Add(-24 * time.Hour)

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev}

	got := entry.ScheduleFirst(now)
	want := naturalNext.Add(24 * time.Hour) // original behavior: rolled forward regardless
	if !got.Equal(want) {
		t.Errorf("ScheduleFirst() = %v, want %v (recovery disabled, original behavior preserved)", got, want)
	}
}

func TestParseRecoveryTimeLimit(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset", "", 0},
		{"valid", "20m", 20 * time.Minute},
		{"invalid", "not-a-duration", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("RECOVERY_TIME_LIMIT", tt.env)
			if got := parseRecoveryTimeLimit(); got != tt.want {
				t.Errorf("parseRecoveryTimeLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}

// RecoveryObserver should fire with recovered=true, carrying the entry's
// RefID, when a recent miss is recovered.
func TestRecoveryObserverCalledOnRecovery(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)

	var gotRefID string
	var gotRecovered bool
	var callCount int
	withRecoveryObserver(t, func(refID string, recovered bool, naturalNext, now time.Time) {
		callCount++
		gotRefID = refID
		gotRecovered = recovered
	})

	now := time.Now()
	naturalNext := now.Add(-5 * time.Minute)
	prev := naturalNext.Add(-24 * time.Hour)

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev, RefID: "retl:src-1:sub-1"}
	entry.ScheduleFirst(now)

	if callCount != 1 {
		t.Fatalf("RecoveryObserver called %d times, want 1", callCount)
	}
	if !gotRecovered {
		t.Error("RecoveryObserver recovered = false, want true")
	}
	if gotRefID != "retl:src-1:sub-1" {
		t.Errorf("RecoveryObserver refID = %q, want %q", gotRefID, "retl:src-1:sub-1")
	}
}

// RecoveryObserver should also fire with recovered=false for a miss beyond
// recoveryTimeLimit, so callers can alert on unrecovered misses too.
func TestRecoveryObserverCalledOnUnrecoveredMiss(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)

	var gotRecovered bool
	var callCount int
	withRecoveryObserver(t, func(refID string, recovered bool, naturalNext, now time.Time) {
		callCount++
		gotRecovered = recovered
	})

	now := time.Now()
	naturalNext := now.Add(-30 * time.Minute) // beyond the 20m limit
	prev := naturalNext.Add(-24 * time.Hour)

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev, RefID: "retl:src-2:sub-2"}
	entry.ScheduleFirst(now)

	if callCount != 1 {
		t.Fatalf("RecoveryObserver called %d times, want 1", callCount)
	}
	if gotRecovered {
		t.Error("RecoveryObserver recovered = true, want false")
	}
}

// RecoveryObserver should not be called at all when there's no miss.
func TestRecoveryObserverNotCalledWhenNotDue(t *testing.T) {
	withRecoveryTimeLimit(t, 20*time.Minute)

	var callCount int
	withRecoveryObserver(t, func(refID string, recovered bool, naturalNext, now time.Time) {
		callCount++
	})

	now := time.Now()
	naturalNext := now.Add(1 * time.Hour) // not due yet
	prev := naturalNext.Add(-24 * time.Hour)

	entry := Entry{Schedule: fakeSchedule{next: naturalNext}, Prev: prev}
	entry.ScheduleFirst(now)

	if callCount != 0 {
		t.Errorf("RecoveryObserver called %d times, want 0", callCount)
	}
}
