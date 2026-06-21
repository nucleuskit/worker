package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var ErrJobNotFound = errors.New("worker job not found")
var ErrInvalidSchedule = errors.New("worker job schedule invalid")

type JobConfig struct {
	Name     string
	Job      string
	Schedule string
	Interval time.Duration
	Timeout  time.Duration
	Metadata map[string]string
}

type ManagerOption func(*managerOptions)

type managerOptions struct {
	hooks []Hook
}

type Manager struct {
	mu      sync.Mutex
	closed  bool
	runners map[string]managedRunner
}

type managedRunner interface {
	Run(context.Context) error
	RunOnce(context.Context) error
	Close() error
}

type scheduleSpec struct {
	interval time.Duration
	schedule cron.Schedule
}

func NewManagerFromConfig(configs []JobConfig, registry map[string]Job, options ...ManagerOption) (*Manager, error) {
	opts := managerOptions{}
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}

	manager := &Manager{runners: make(map[string]managedRunner, len(configs))}
	for _, config := range configs {
		name := strings.TrimSpace(config.Name)
		jobKey := strings.TrimSpace(config.Job)
		if name == "" {
			name = jobKey
		}
		job, ok := registry[jobKey]
		if !ok || job == nil {
			return nil, fmt.Errorf("%w: %s", ErrJobNotFound, jobKey)
		}
		if _, exists := manager.runners[name]; exists {
			return nil, fmt.Errorf("worker job %q already registered", name)
		}
		schedule, err := parseJobSchedule(config)
		if err != nil {
			return nil, err
		}
		cronOptions := []CronOption{
			WithJobName(name),
			WithJobMetadata(config.Metadata),
		}
		if config.Timeout > 0 {
			cronOptions = append(cronOptions, WithJobTimeout(config.Timeout))
		}
		for _, hook := range opts.hooks {
			cronOptions = append(cronOptions, WithCronHook(hook))
		}
		runner := NewCronRunner(job, schedule.interval, cronOptions...)
		if schedule.schedule != nil {
			manager.runners[name] = &cronExpressionRunner{runner: runner, schedule: schedule.schedule}
			continue
		}
		manager.runners[name] = runner
	}
	return manager, nil
}

func WithManagerHook(hook Hook) ManagerOption {
	return func(opts *managerOptions) {
		if hook != nil {
			opts.hooks = append(opts.hooks, hook)
		}
	}
}

func (m *Manager) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runners, err := m.snapshotRunners()
	if err != nil {
		return err
	}
	if len(runners) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	errs := make(chan error, len(runners))
	var wg sync.WaitGroup
	wg.Add(len(runners))
	for _, runner := range runners {
		runner := runner
		go func() {
			defer wg.Done()
			if err := runner.Run(ctx); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (m *Manager) RunOnce(ctx context.Context, name string) error {
	runner, err := m.runner(name)
	if err != nil {
		return err
	}
	return runner.RunOnce(ctx)
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	runners := make([]managedRunner, 0, len(m.runners))
	for _, runner := range m.runners {
		runners = append(runners, runner)
	}
	m.mu.Unlock()

	var joined error
	for _, runner := range runners {
		joined = errors.Join(joined, runner.Close())
	}
	return joined
}

func (m *Manager) snapshotRunners() ([]managedRunner, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrRunnerClosed
	}
	runners := make([]managedRunner, 0, len(m.runners))
	for _, runner := range m.runners {
		runners = append(runners, runner)
	}
	return runners, nil
}

func (m *Manager) runner(name string) (managedRunner, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runner, ok := m.runners[strings.TrimSpace(name)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, name)
	}
	return runner, nil
}

func parseJobSchedule(config JobConfig) (scheduleSpec, error) {
	if config.Interval > 0 {
		return scheduleSpec{interval: normalizeCronInterval(config.Interval)}, nil
	}
	schedule := strings.TrimSpace(config.Schedule)
	if schedule == "" {
		return scheduleSpec{interval: normalizeCronInterval(0)}, nil
	}
	if strings.HasPrefix(schedule, "@every ") {
		duration, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(schedule, "@every ")))
		if err != nil || duration <= 0 {
			return scheduleSpec{}, fmt.Errorf("%w: %s", ErrInvalidSchedule, schedule)
		}
		return scheduleSpec{interval: duration}, nil
	}
	duration, err := time.ParseDuration(schedule)
	if err == nil && duration > 0 {
		return scheduleSpec{interval: duration}, nil
	}
	cronSchedule, err := cronParser().Parse(schedule)
	if err != nil {
		return scheduleSpec{}, fmt.Errorf("%w: %s", ErrInvalidSchedule, schedule)
	}
	return scheduleSpec{interval: normalizeCronInterval(0), schedule: cronSchedule}, nil
}

func cronParser() cron.Parser {
	return cron.NewParser(
		cron.SecondOptional |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
	)
}

type cronExpressionRunner struct {
	runner   *CronRunner
	schedule cron.Schedule
}

func (r *cronExpressionRunner) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		next := r.schedule.Next(time.Now())
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-r.runner.stop():
			stopTimer(timer)
			return ErrRunnerClosed
		case <-timer.C:
			if err := r.runner.RunOnce(ctx); err != nil && !errors.Is(err, ErrRunnerClosed) {
				return err
			}
		}
	}
}

func (r *cronExpressionRunner) RunOnce(ctx context.Context) error {
	return r.runner.RunOnce(ctx)
}

func (r *cronExpressionRunner) Close() error {
	return r.runner.Close()
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
