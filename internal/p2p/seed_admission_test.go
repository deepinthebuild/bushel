package p2p

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"pgregory.net/rapid"
)

func TestSeedConfigurationBounds(t *testing.T) {
	limits, err := seedConfiguration(Config{})
	if err != nil || limits.maxBlob != 256<<20 || limits.hourlyBytes != 1<<30 {
		t.Fatalf("seed defaults: %+v %v", limits, err)
	}
	for _, size := range []int64{-1, cas.MaxBlobSize + 1, math.MaxInt64} {
		if _, err := seedConfiguration(Config{SeedMaxBlobBytes: size}); err == nil {
			t.Fatalf("accepted blob limit %d", size)
		}
	}
	for _, size := range []int64{-1, 1<<40 + 1, math.MaxInt64} {
		if _, err := seedConfiguration(Config{SeedBytesPerHour: size}); err == nil {
			t.Fatalf("accepted hourly limit %d", size)
		}
	}
	if _, err := seedConfiguration(Config{SeedBytesPerHour: 1 << 40}); err != nil {
		t.Fatal(err)
	}
}

func TestPropertySeedAdmissionRollingBudget(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limits := seedLimits{maxBlob: rapid.Int64Range(1, 32).Draw(t, "maximum blob"), hourlyBytes: rapid.Int64Range(1, 64).Draw(t, "hourly bytes")}
		state := seedAdmission{limits: limits}
		type charge struct {
			at   time.Time
			size int64
		}
		var charges []charge
		active := make(map[cas.Digest]bool)
		now := time.Unix(0, 0)
		for _, event := range rapid.SliceOfN(rapid.IntRange(0, 15), 1, 150).Draw(t, "events") {
			d := cas.Sum([]byte{byte(event / 4)})
			d.Size = int64(event/4 + 1)
			if event/4 == 0 {
				d = cas.Sum(nil)
			}
			switch event % 4 {
			case 0, 1:
				var used int64
				for _, c := range charges {
					if now.Sub(c.at) < time.Hour {
						used += c.size
					}
				}
				want := !active[d] && len(active) < seedConcurrency && d.Size <= limits.maxBlob && used+d.Size <= limits.hourlyBytes
				if accepted := state.admit(now, d); accepted != want {
					t.Fatalf("admission diverged: accepted=%t want=%t state=%+v", accepted, want, state)
				}
				if want {
					active[d] = true
					charges = append(charges, charge{now, d.Size})
				}
			case 2:
				state.finish(d)
				delete(active, d)
			case 3:
				now = now.Add(time.Duration(rapid.IntRange(0, 7200).Draw(t, "elapsed seconds")) * time.Second)
			}
			if len(state.active) != len(active) || len(state.active) > seedConcurrency || state.used < 0 || state.used > limits.hourlyBytes || len(state.charges) > seedChargeLimit {
				t.Fatal("admission bound failed")
			}
		}
	})
}

func TestSeedAdmissionChargesFailuresAndBoundsEmptyOffers(t *testing.T) {
	state := seedAdmission{limits: seedLimits{maxBlob: 16, hourlyBytes: 16}}
	now := time.Unix(0, 0)
	d := cas.Sum([]byte("0123456789abcdef"))
	if !state.admit(now, d) {
		t.Fatal("initial admission rejected")
	}
	state.finish(d)
	if state.admit(now.Add(time.Hour-time.Nanosecond), d) {
		t.Fatal("failed transfer refunded hourly bytes")
	}
	if !state.admit(now.Add(time.Hour), d) {
		t.Fatal("rolling charge did not expire")
	}
	state.finish(d)
	if state.admit(now, d) {
		t.Fatal("backward clock step restored allowance")
	}
	empty := cas.Sum(nil)
	for i := len(state.charges); i < seedChargeLimit; i++ {
		if !state.admit(now.Add(time.Hour), empty) {
			t.Fatal("bounded empty admission rejected early")
		}
		state.finish(empty)
	}
	if state.admit(now.Add(time.Hour), empty) {
		t.Fatal("empty offers exceeded charge count bound")
	}
}

func TestPropertySeedControlRate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var rate seedRate
		now := time.Time{}
		accepted := 0
		elapsed := time.Duration(0)
		for _, delta := range rapid.SliceOfN(rapid.IntRange(0, 1500), 1, 200).Draw(t, "milliseconds") {
			step := time.Duration(delta) * time.Millisecond
			now = now.Add(step)
			elapsed += step
			if rate.allow(now) {
				accepted++
			}
			if accepted > seedBurst+int(elapsed/time.Second) || rate.tokens < 0 || rate.tokens >= seedBurst {
				t.Fatal("control token conservation failed")
			}
		}
	})
}

func FuzzSeedOfferParsing(f *testing.F) {
	for _, size := range []uint64{0, 1, 256 << 20, uint64(cas.MaxBlobSize), uint64(cas.MaxBlobSize) + 1, 1 << 63, math.MaxUint64} {
		d := cas.Sum(nil)
		var request [40]byte
		copy(request[:32], d.Hash[:])
		binary.BigEndian.PutUint64(request[32:], size)
		f.Add(request[:])
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, request []byte) {
		d, err := parseSeedOffer(request)
		if len(request) != 40 {
			if err == nil {
				t.Fatal("accepted a non-fixed-size offer")
			}
			return
		}
		size := binary.BigEndian.Uint64(request[32:])
		if err == nil && (size > uint64(cas.MaxBlobSize) || d.Validate() != nil || uint64(d.Size) != size) {
			t.Fatal("offer integer bounds failed")
		}
	})
}
