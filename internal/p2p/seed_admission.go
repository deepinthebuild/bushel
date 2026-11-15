package p2p

import (
	"errors"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
)

const (
	seedConcurrency = 2
	seedControls    = 8
	seedBurst       = 16
	seedChargeLimit = 4096
	seedCandidates  = 16
)

type seedLimits struct{ maxBlob, hourlyBytes int64 }

func seedConfiguration(cfg Config) (seedLimits, error) {
	limits := seedLimits{cfg.SeedMaxBlobBytes, cfg.SeedBytesPerHour}
	if limits.maxBlob == 0 {
		limits.maxBlob = 256 << 20
	}
	if limits.hourlyBytes == 0 {
		limits.hourlyBytes = 1 << 30
	}
	if limits.maxBlob < 1 || limits.maxBlob > cas.MaxBlobSize {
		return seedLimits{}, errors.New("seed blob limit must be positive and at most the maximum blob size")
	}
	if limits.hourlyBytes < 1 || limits.hourlyBytes > 1<<40 {
		return seedLimits{}, errors.New("seed hourly limit must be positive and at most 1 TiB")
	}
	return limits, nil
}

// seedRate permits a burst of 16 controls and then one per second. Its caller
// supplies time and synchronization; backward clock steps cannot add credit.
type seedRate struct {
	at      time.Time
	tokens  int
	started bool
}

func (r *seedRate) allow(now time.Time) bool {
	if !r.started {
		r.at, r.tokens = now, seedBurst
		r.started = true
	}
	if elapsed := now.Sub(r.at); elapsed >= time.Second {
		added := elapsed / time.Second
		r.tokens = min(seedBurst, r.tokens+int(added))
		r.at = r.at.Add(added * time.Second)
	}
	if r.tokens == 0 {
		return false
	}
	r.tokens--
	return true
}

type seedCharge struct {
	at    time.Time
	bytes int64
}
type seedAdmission struct {
	limits  seedLimits
	active  map[cas.Digest]struct{}
	charges []seedCharge
	used    int64
	last    time.Time
}

// admit reserves the entire claimed size for a rolling hour. Failed or
// canceled pulls keep their charge, bounding repeated partial transfers. This
// receiver allowance is process-local and starts fresh after a daemon restart.
// Entries with zero bytes remain bounded by control rate and charge count.
func (a *seedAdmission) admit(now time.Time, d cas.Digest) bool {
	if now.Before(a.last) {
		now = a.last
	}
	a.last = now
	expired := 0
	for expired < len(a.charges) && !a.charges[expired].at.Add(time.Hour).After(now) {
		a.used -= a.charges[expired].bytes
		expired++
	}
	if expired > 0 {
		copy(a.charges, a.charges[expired:])
		a.charges = a.charges[:len(a.charges)-expired]
	}
	if d.Validate() != nil || d.Size > a.limits.maxBlob || d.Size > a.limits.hourlyBytes-a.used || len(a.active) >= seedConcurrency || len(a.charges) >= seedChargeLimit {
		return false
	}
	if _, exists := a.active[d]; exists {
		return false
	}
	if a.active == nil {
		a.active = make(map[cas.Digest]struct{}, seedConcurrency)
	}
	a.active[d] = struct{}{}
	a.used += d.Size
	a.charges = append(a.charges, seedCharge{now, d.Size})
	return true
}

func (a *seedAdmission) finish(d cas.Digest) { delete(a.active, d) }
