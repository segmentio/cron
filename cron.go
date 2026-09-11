package cron

import (
	"context"
	"os"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run(ctx context.Context)
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time.
	// Next is invoked initially, and then each time the job is run.
	// `from` is the starting time instant to begin the delay calculation.
	Next(from time.Time) time.Time

	// NextWithAfter returns the next activation time, later than the after time.
	// If `after` time provided is non-zero then the activation time returned will be later than `after`.
	// `from` is the starting time instant to begin the delay calculation
	// `after` is the time instant the calculated next instant should be after
	NextWithAfter(from, after time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// AdHocInvokedTime will record the invocation time of manually triggered job runs.
	AdHocInvokedTime time.Time

	// RefID is an opaque, caller-supplied identifier for this entry (set via
	// WithRefID), passed through to RecoveryObserver so callers can tag their
	// own metrics/logs with whatever identifies this job in their system
	// (e.g. "retl:<source_id>:<subscription_id>"). This package never
	// interprets it.
	RefID string

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// recoveryTimeLimit bounds how late a freshly-registered entry's natural next
// tick (i.e. Schedule.Next(Prev)) can already be, in ScheduleFirst, before it
// is fired almost immediately instead of being silently rolled forward to its
// next scheduled occurrence.
//
// This is zero (disabled) by default, which preserves this library's original
// behavior for every existing consumer. Set RECOVERY_TIME_LIMIT to a duration
// string accepted by time.ParseDuration (e.g. "20m") to opt in.
//
// Background: ScheduleFirst calls Schedule.NextWithAfter(Prev, now), and
// NextWithAfter starts its search from whichever of its two arguments is
// later. Since `now` is virtually always later than `Prev`, the search
// effectively always starts from `now` - so if the entry's natural next tick
// (computed from Prev) already passed by the time it's (re-)registered, it is
// silently skipped in favor of the following occurrence, with no error and no
// signal that anything was missed. This is most likely to bite right after a
// process restart, when every entry has to be freshly re-registered at once.
var recoveryTimeLimit = parseRecoveryTimeLimit()

// recoveryFireDelay is how soon a recovered entry (one whose natural next
// tick was found to be overdue, but within recoveryTimeLimit) is scheduled to
// fire once RECOVERY_TIME_LIMIT is set.
const recoveryFireDelay = 30 * time.Second

func parseRecoveryTimeLimit() time.Duration {
	v := os.Getenv("RECOVERY_TIME_LIMIT")
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0
	}
	return d
}

// RecoveryObserver, when set, is called by ScheduleFirst whenever a
// re-registered entry's natural next tick (Schedule.Next(Prev)) is found to
// already be overdue - only once RECOVERY_TIME_LIMIT is set, since that's
// what makes this check run at all. recovered is true if the miss was within
// RECOVERY_TIME_LIMIT (so the entry was scheduled to fire almost
// immediately), false if it was older (so it was left to roll forward to its
// next occurrence as usual, same as this package's original behavior).
//
// refID is whatever the caller attached to the entry via WithRefID - this
// package never interprets it, it's just plumbed through so a caller can
// tag their own metric/log with it (e.g. a source/subscription identifier)
// without this package needing to know what that means.
//
// Nil by default (no-op). Not safe to reassign concurrently with a running
// Cron; set it once during startup before adding jobs.
var RecoveryObserver func(refID string, recovered bool, naturalNext, now time.Time)

// ScheduleFirst is used for the initial scheduling. If a Prev value has been
// included with the Entry, it will be used in place of "now" to allow schedules
// to be preserved across process restarts.
func (e Entry) ScheduleFirst(now time.Time) time.Time {
	if e.Prev.IsZero() {
		return e.Schedule.NextWithAfter(now, time.Time{})
	}
	if fireAt, ok := e.recoveryFireTime(now); ok {
		return fireAt
	}
	return e.Schedule.NextWithAfter(e.Prev, now)
}

// recoveryFireTime reports whether e's natural next tick (Schedule.Next(Prev))
// has already passed by now, and if so notifies RecoveryObserver and, when the
// miss is within recoveryTimeLimit, returns the almost-immediate time it
// should fire at instead of being silently rolled forward by NextWithAfter.
// ok is false - leaving ScheduleFirst to its original NextWithAfter behavior -
// whenever recovery is disabled, there's no miss, or the miss is too old to
// recover.
func (e Entry) recoveryFireTime(now time.Time) (fireAt time.Time, ok bool) {
	if recoveryTimeLimit <= 0 {
		return time.Time{}, false
	}
	naturalNext := e.Schedule.Next(e.Prev)
	if naturalNext.IsZero() || !naturalNext.Before(now) {
		return time.Time{}, false
	}

	recovered := now.Sub(naturalNext) <= recoveryTimeLimit
	if RecoveryObserver != nil {
		RecoveryObserver(e.RefID, recovered, naturalNext, now)
	}
	if !recovered {
		return time.Time{}, false
	}
	return now.Add(recoveryFireDelay), true
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//	Time Zone
//	  Description: The time zone in which schedules are interpreted
//	  Default:     time.Local
//
//	Parser
//	  Description: Parser converts cron spec strings into cron.Schedules.
//	  Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//	Chain
//	  Description: Wrap submitted jobs to customize behavior.
//	  Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func(ctx context.Context)

func (f FuncJob) Run(ctx context.Context) { f(ctx) }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func(context.Context), entryOpts ...EntryOption) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd), entryOpts...)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job, entryOpts ...EntryOption) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd, entryOpts...), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job, entryOpts ...EntryOption) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	for _, fn := range entryOpts {
		fn(entry)
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// EntryOption is a hook which allows the Entry to be altered before being
// committed internally.
type EntryOption func(*Entry)

// EntryPrev allows setting the Prev time to allow interval-based schedules to
// preserve their timeline even in the face of process restarts.
func WithPrev(prev time.Time) EntryOption {
	return func(e *Entry) {
		e.Prev = prev
	}
}

// WithRefID attaches an opaque, caller-defined identifier to an entry. It has
// no effect on scheduling; it exists purely so RecoveryObserver can report
// which entry a recovered or unrecovered miss belongs to, in whatever form
// the caller finds useful (e.g. "retl:<source_id>:<subscription_id>").
func WithRefID(id string) EntryOption {
	return func(e *Entry) {
		e.RefID = id
	}
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// SetInvocationTimeForEntry allows updating an entry with invocation time for manually triggered runs. This can be used
// for calculation of execution start delay/latency metric.
func (c *Cron) SetInvocationTimeForEntry(id EntryID, invocationTime time.Time) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	for _, entry := range c.entries {
		if entry.ID == id {
			entry.AdHocInvokedTime = invocationTime
		}
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start(ctx context.Context) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run(ctx)
}

// Run the cron scheduler, or no-op if already running. It blocks until the
// given context is canceled.
func (c *Cron) Run(ctx context.Context) {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run(ctx)
}

// run the scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(ctx context.Context) {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.ScheduleFirst(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					e.Prev = e.Next
					e.Next = e.Schedule.NextWithAfter(e.Prev, now)
					e.AdHocInvokedTime = time.Time{} // reset the adhoc invoked time if it was set previous to this run.
					c.startJob(ctx, e.WrappedJob)
					c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.ScheduleFirst(now)
				c.entries = append(c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-ctx.Done():
				timer.Stop()
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)
			}

			break
		}
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(ctx context.Context, j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run(ctx)
	}()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Wait waits for all running jobs to exit. This can be used with context
// cancellation to gracefully stop the run loop.
func (c *Cron) Wait() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.jobWaiter.Wait()
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	entries := make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}
