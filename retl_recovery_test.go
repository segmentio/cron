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

	got := entry.ScheduleFirst(now)
	want := now.Add(recoveryFireDelay)
	if !got.Equal(want) {
		t.Errorf("ScheduleFirst() = %v, want %v (recovered fire time)", got, want)
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
// doesn't opt in via RETL_RECOVERY_TIME_LIMIT.
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
			t.Setenv("RETL_RECOVERY_TIME_LIMIT", tt.env)
			if got := parseRecoveryTimeLimit(); got != tt.want {
				t.Errorf("parseRecoveryTimeLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}
