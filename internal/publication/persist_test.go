package publication

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	publicationpb "github.com/deepinthebuild/bushel/internal/publication/pb"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

func snapshotFixture() *state {
	s := newState(time.Unix(100000, 0))
	s.observe(publicationDigest(1, 1024), association(1, workspace.Canonical), true)
	s.observe(publicationDigest(2, 2048), association(2, workspace.Main), false)
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 4096)
	s.finish(*a, true, false)
	return s
}

func encodedFixture(t testing.TB) []byte {
	t.Helper()
	b, err := marshalState(snapshotFixture())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func marshalProto(t testing.TB, m proto.Message) []byte {
	t.Helper()
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func protoVarintField(number protowire.Number, value uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, number, protowire.VarintType), value)
}

func protoBytesField(number protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
}

// Replace a field in an already valid fixture, leaving its other wire bytes
// intact so malformed tests exercise the decoder rather than the encoder.
func replaceProtoField(t testing.TB, b []byte, number protowire.Number, replacement []byte) []byte {
	t.Helper()
	var result []byte
	found := false
	for len(b) > 0 {
		n, typ, tagSize := protowire.ConsumeTag(b)
		if tagSize < 0 {
			t.Fatal("invalid fixture tag")
		}
		valueSize := protowire.ConsumeFieldValue(n, typ, b[tagSize:])
		if valueSize < 0 {
			t.Fatal("invalid fixture value")
		}
		size := tagSize + valueSize
		if n == number {
			if !found {
				result = append(result, replacement...)
			}
			found = true
		} else {
			result = append(result, b[:size]...)
		}
		b = b[size:]
	}
	if !found {
		t.Fatalf("fixture has no field %d", number)
	}
	return result
}

func snapshotProtoFixture(t testing.TB) *publicationpb.Snapshot {
	t.Helper()
	value := new(publicationpb.Snapshot)
	if err := proto.Unmarshal(encodedFixture(t), value); err != nil {
		t.Fatal(err)
	}
	return value
}

func protoPeer(id int) *publicationpb.PeerID {
	return &publicationpb.PeerID{Multihash: []byte(publicationPeer(id))}
}

func TestPreflightMatchesGeneratedSchema(t *testing.T) {
	definitions := []struct {
		message proto.Message
		kind    wireMessage
		fields  []wireField
	}{
		{&publicationpb.Digest{}, digestWire, digestFields[:]},
		{&timestamppb.Timestamp{}, timestampWire, timestampFields[:]},
		{&publicationpb.PeerID{}, peerIDWire, peerIDFields[:]},
		{&publicationpb.Association{}, associationWire, associationFields[:]},
		{&publicationpb.ServiceCredit{}, serviceCreditWire, serviceCreditFields[:]},
		{&publicationpb.Job{}, jobWire, jobFields[:]},
		{&publicationpb.Snapshot{}, snapshotWire, snapshotFields[:]},
		{&publicationpb.JournalEntry{}, entryWire, entryFields[:]},
	}
	children := make(map[wireMessage]protoreflect.FullName)
	for _, definition := range definitions {
		children[definition.kind] = definition.message.ProtoReflect().Descriptor().FullName()
	}
	// Repeated fields and opaque bytes need explicit application bounds that
	// protobuf itself cannot express. A new schema field requires review here.
	counts := map[protoreflect.FullName]int{
		"bushel.publication.v1.Job.associations":     4,
		"bushel.publication.v1.Job.attempted_peers":  maxAttempts,
		"bushel.publication.v1.Job.completed_peers":  ReplicaTarget,
		"bushel.publication.v1.Snapshot.served":      fairBuckets,
		"bushel.publication.v1.Snapshot.jobs":        maxRecords,
		"bushel.publication.v1.JournalEntry.deleted": maxRecords,
	}
	byteLimits := map[protoreflect.FullName]int{
		"bushel.publication.v1.Digest.sha256":                32,
		"bushel.publication.v1.PeerID.multihash":             128,
		"bushel.publication.v1.Association.partition_sha256": 32,
	}
	for _, definition := range definitions {
		descriptor := definition.message.ProtoReflect().Descriptor()
		t.Run(string(descriptor.FullName()), func(t *testing.T) {
			fields := descriptor.Fields()
			if fields.Len() != len(definition.fields)-1 {
				t.Fatal("preflight field inventory differs from generated descriptor")
			}
			for i := 0; i < fields.Len(); i++ {
				field := fields.Get(i)
				if field.Number() <= 0 || int(field.Number()) >= len(definition.fields) {
					t.Fatalf("field %s has no preflight definition", field.FullName())
				}
				bound := definition.fields[field.Number()]
				count := 1
				if field.IsList() {
					count = counts[field.FullName()]
					if count < 2 || field.IsPacked() {
						t.Fatalf("field %s needs an explicit supported collection bound", field.FullName())
					}
				}
				if bound.count != count {
					t.Fatalf("field %s occurrence cap is %d, want %d", field.FullName(), bound.count, count)
				}
				wireKind := protowire.VarintType
				switch field.Kind() {
				case protoreflect.MessageKind:
					wireKind = protowire.BytesType
					if children[bound.child] != field.Message().FullName() {
						t.Fatalf("field %s scans the wrong nested message", field.FullName())
					}
				case protoreflect.BytesKind:
					wireKind = protowire.BytesType
					if byteLimits[field.FullName()] == 0 || bound.bytes != byteLimits[field.FullName()] {
						t.Fatalf("field %s needs the declared opaque-byte limit", field.FullName())
					}
				case protoreflect.EnumKind:
					values := field.Enum().Values()
					var maximum uint64
					for i := 0; i < values.Len(); i++ {
						if values.Get(i).Number() < 0 {
							t.Fatalf("field %s introduced a negative enum value", field.FullName())
						}
						maximum = max(maximum, uint64(values.Get(i).Number()))
					}
					if bound.value != maximum {
						t.Fatalf("field %s enum bound differs from generated values", field.FullName())
					}
				case protoreflect.BoolKind, protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind:
				default:
					t.Fatalf("field %s introduced unsupported kind %s", field.FullName(), field.Kind())
				}
				if bound.kind != wireKind || field.Kind() != protoreflect.MessageKind && bound.child != 0 {
					t.Fatalf("field %s preflight wire type differs from its descriptor", field.FullName())
				}
			}
		})
	}
}

func TestSnapshotRejectsInvalidData(t *testing.T) {
	mutations := map[string]func(*publicationpb.Snapshot){
		"missing version":          func(s *publicationpb.Snapshot) { s.Version = 0 },
		"unsupported version":      func(s *publicationpb.Snapshot) { s.Version++ },
		"missing clock":            func(s *publicationpb.Snapshot) { s.Clock = nil },
		"negative clock":           func(s *publicationpb.Snapshot) { s.Clock.Seconds = -1 },
		"clock overflow":           func(s *publicationpb.Snapshot) { s.Clock.Seconds = math.MaxInt64 },
		"clock beyond bound":       func(s *publicationpb.Snapshot) { s.Clock.Seconds = 253402214400 },
		"fractional clock":         func(s *publicationpb.Snapshot) { s.Clock.Nanos = 1 },
		"negative clock nanos":     func(s *publicationpb.Snapshot) { s.Clock.Nanos = -1 },
		"missing budget window":    func(s *publicationpb.Snapshot) { s.Window = nil },
		"future budget window":     func(s *publicationpb.Snapshot) { s.Window.Seconds = s.Clock.Seconds + 1 },
		"expired budget window":    func(s *publicationpb.Snapshot) { s.Window.Seconds = s.Clock.Seconds - 3600 },
		"fractional budget window": func(s *publicationpb.Snapshot) { s.Window.Nanos = 1 },
		"charged bytes overflow":   func(s *publicationpb.Snapshot) { s.ChargedBytes = math.MaxUint64 },
		"unearned charged bytes":   func(s *publicationpb.Snapshot) { s.ChargedOffers = 0 },
		"too many offers":          func(s *publicationpb.Snapshot) { s.ChargedOffers = maxOffersPerHour + 1 },
		"missing cooldown":         func(s *publicationpb.Snapshot) { s.NextStart = nil },
		"unbounded cooldown":       func(s *publicationpb.Snapshot) { s.NextStart.Seconds = s.Clock.Seconds + 2 },
		"cooldown overflow":        func(s *publicationpb.Snapshot) { s.NextStart.Seconds = math.MaxInt64 },
		"fractional cooldown":      func(s *publicationpb.Snapshot) { s.NextStart.Nanos = 1 },
		"zero service credit":      func(s *publicationpb.Snapshot) { s.Served = []*publicationpb.ServiceCredit{{Bucket: 1}} },
		"unknown service bucket": func(s *publicationpb.Snapshot) {
			s.Served = []*publicationpb.ServiceCredit{{Bucket: fairBuckets, ChargedBytes: 1}}
		},
		"duplicate service bucket": func(s *publicationpb.Snapshot) { s.Served = append(s.Served, s.Served[0]) },
		"reordered service buckets": func(s *publicationpb.Snapshot) {
			s.Served = []*publicationpb.ServiceCredit{{Bucket: 2, ChargedBytes: 1}, {Bucket: 1, ChargedBytes: 1}}
		},
		"duplicate job":          func(s *publicationpb.Snapshot) { s.Jobs = append(s.Jobs, s.Jobs[0]) },
		"reordered jobs":         func(s *publicationpb.Snapshot) { s.Jobs[0], s.Jobs[1] = s.Jobs[1], s.Jobs[0] },
		"missing digest":         func(s *publicationpb.Snapshot) { s.Jobs[0].Digest = nil },
		"short digest":           func(s *publicationpb.Snapshot) { s.Jobs[0].Digest.Sha256 = make([]byte, 31) },
		"long digest":            func(s *publicationpb.Snapshot) { s.Jobs[0].Digest.Sha256 = make([]byte, 33) },
		"blob size overflow":     func(s *publicationpb.Snapshot) { s.Jobs[0].Digest.SizeBytes = math.MaxUint64 },
		"blob size beyond bound": func(s *publicationpb.Snapshot) { s.Jobs[0].Digest.SizeBytes = uint64(cas.MaxBlobSize) + 1 },
		"wrong empty digest":     func(s *publicationpb.Snapshot) { s.Jobs[0].Digest.SizeBytes = 0 },
		"missing creation":       func(s *publicationpb.Snapshot) { s.Jobs[0].Created = nil },
		"future creation":        func(s *publicationpb.Snapshot) { s.Jobs[0].Created.Seconds = s.Clock.Seconds + 1 },
		"creation overflow":      func(s *publicationpb.Snapshot) { s.Jobs[0].Created.Seconds = math.MaxInt64 },
		"fractional creation":    func(s *publicationpb.Snapshot) { s.Jobs[0].Created.Nanos = 1 },
		"missing due":            func(s *publicationpb.Snapshot) { s.Jobs[0].Due = nil },
		"past due":               func(s *publicationpb.Snapshot) { s.Jobs[0].Due.Seconds = s.Jobs[0].Created.Seconds - 1 },
		"unbounded due":          func(s *publicationpb.Snapshot) { s.Jobs[0].Due.Seconds = s.Clock.Seconds + 3601 },
		"fractional due":         func(s *publicationpb.Snapshot) { s.Jobs[0].Due.Nanos = 1 },
		"no association":         func(s *publicationpb.Snapshot) { s.Jobs[0].Associations = nil },
		"unknown class":          func(s *publicationpb.Snapshot) { s.Jobs[0].Associations[0].Kind = 4 },
		"negative class":         func(s *publicationpb.Snapshot) { s.Jobs[0].Associations[0].Kind = -1 },
		"short partition":        func(s *publicationpb.Snapshot) { s.Jobs[0].Associations[0].PartitionSha256 = make([]byte, 31) },
		"long partition":         func(s *publicationpb.Snapshot) { s.Jobs[0].Associations[0].PartitionSha256 = make([]byte, 33) },
		"duplicate association": func(s *publicationpb.Snapshot) {
			s.Jobs[0].Associations = append(s.Jobs[0].Associations, s.Jobs[0].Associations[0])
		},
		"reordered associations": func(s *publicationpb.Snapshot) {
			s.Jobs[0].Associations = []*publicationpb.Association{
				{Kind: 3, PartitionSha256: make([]byte, 32)}, {Kind: 1, PartitionSha256: make([]byte, 32)},
			}
		},
		"invalid peer": func(s *publicationpb.Snapshot) {
			s.Jobs[0].AttemptedPeers = []*publicationpb.PeerID{{Multihash: []byte("not-a-peer")}}
		},
		"empty peer": func(s *publicationpb.Snapshot) { s.Jobs[0].AttemptedPeers = []*publicationpb.PeerID{{}} },
		"overlong peer": func(s *publicationpb.Snapshot) {
			s.Jobs[0].AttemptedPeers = []*publicationpb.PeerID{{Multihash: make([]byte, 129)}}
		},
		"per peer attempt limit": func(s *publicationpb.Snapshot) {
			s.Jobs[0].AttemptedPeers = []*publicationpb.PeerID{protoPeer(1), protoPeer(1), protoPeer(1)}
		},
		"total attempt limit": func(s *publicationpb.Snapshot) {
			s.Jobs[0].AttemptedPeers = nil
			for i := range maxAttempts + 1 {
				s.Jobs[0].AttemptedPeers = append(s.Jobs[0].AttemptedPeers, protoPeer(i+1))
			}
		},
		"unattempted receipt": func(s *publicationpb.Snapshot) { s.Jobs[0].CompletedPeers = []*publicationpb.PeerID{protoPeer(99)} },
		"duplicate receipt": func(s *publicationpb.Snapshot) {
			s.Jobs[0].AttemptedPeers = []*publicationpb.PeerID{protoPeer(1)}
			s.Jobs[0].CompletedPeers = []*publicationpb.PeerID{protoPeer(1), protoPeer(1)}
		},
		"too many receipts": func(s *publicationpb.Snapshot) {
			s.Jobs[0].AttemptedPeers = []*publicationpb.PeerID{protoPeer(1), protoPeer(2), protoPeer(3)}
			s.Jobs[0].CompletedPeers = s.Jobs[0].AttemptedPeers
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			value := snapshotProtoFixture(t)
			mutate(value)
			if _, err := parseState(marshalProto(t, value)); err == nil {
				t.Fatal("accepted invalid snapshot")
			}
		})
	}
}

func TestSnapshotRejectsNoncanonicalWire(t *testing.T) {
	fixture := encodedFixture(t)
	version := protoVarintField(1, 1)
	jobValue := snapshotProtoFixture(t).Jobs[0]
	jobValue.Ready = true
	jobValue.AttemptedPeers = []*publicationpb.PeerID{protoPeer(1)}
	job := marshalProto(t, jobValue)
	nestedUnknown := append(bytes.Clone(job), protoVarintField(99, 1)...)
	digest := marshalProto(t, snapshotProtoFixture(t).Jobs[0].Digest)
	nestedDuplicate := replaceProtoField(t, job, 1, protoBytesField(1, append(bytes.Clone(digest), protoBytesField(1, make([]byte, 32))...)))
	noncanonicalReady := replaceProtoField(t, job, 4, protoVarintField(4, 2))
	explicitUnknown := replaceProtoField(t, job, 5, protoBytesField(5, append(protoVarintField(1, 0), protoBytesField(2, make([]byte, 32))...)))
	overlongHash := append([]byte{0x0a, 0xa0, 0x00}, jobValue.Digest.Sha256...)
	overlongHash = append(overlongHash, protoVarintField(2, jobValue.Digest.SizeBytes)...)
	overlongDigest := replaceProtoField(t, job, 1, protoBytesField(1, overlongHash))
	clockBytes := marshalProto(t, snapshotProtoFixture(t).Clock)
	peerBytes := marshalProto(t, protoPeer(1))
	duplicateMultihash := replaceProtoField(t, job, 6, protoBytesField(6, append(bytes.Clone(peerBytes), peerBytes...)))
	for name, bad := range map[string][]byte{
		"duplicate singular":              append(bytes.Clone(fixture), version...),
		"missing version":                 replaceProtoField(t, fixture, 1, nil),
		"explicit default":                append(bytes.Clone(fixture), protoVarintField(8, 0)...),
		"unknown field":                   append(bytes.Clone(fixture), protoVarintField(99, 1)...),
		"wrong wire type":                 replaceProtoField(t, fixture, 1, protoBytesField(1, []byte{1})),
		"out of order":                    append(replaceProtoField(t, fixture, 1, nil), version...),
		"overlong varint":                 replaceProtoField(t, fixture, 1, []byte{8, 0x81, 0}),
		"overlong tag":                    replaceProtoField(t, fixture, 1, []byte{0x88, 0, 1}),
		"overflow varint":                 replaceProtoField(t, fixture, 1, append([]byte{8}, bytes.Repeat([]byte{0xff}, 11)...)),
		"overflow uint32":                 replaceProtoField(t, fixture, 1, protoVarintField(1, 1<<32|1)),
		"zero tag":                        append(bytes.Clone(fixture), 0),
		"invalid wire type":               append(bytes.Clone(fixture), 0x0e),
		"group":                           append(bytes.Clone(fixture), 0x0b, 0x0c),
		"truncated tag":                   append(bytes.Clone(fixture), 0x80),
		"truncated varint":                replaceProtoField(t, fixture, 1, []byte{8, 0x80}),
		"truncated bytes":                 append(bytes.Clone(fixture), 0x4a, 2, 0),
		"overflow length":                 append(bytes.Clone(fixture), append([]byte{0x4a}, bytes.Repeat([]byte{0xff}, 11)...)...),
		"huge declared length":            append(bytes.Clone(fixture), protowire.AppendVarint([]byte{0x4a}, math.MaxUint64)...),
		"packed service counters":         replaceProtoField(t, fixture, 7, protoBytesField(7, make([]byte, fairBuckets))),
		"explicit default service bucket": replaceProtoField(t, fixture, 7, protoBytesField(7, append(protoVarintField(1, 0), protoVarintField(2, 1)...))),
		"duplicate service charge":        replaceProtoField(t, fixture, 7, protoBytesField(7, append(protoVarintField(2, 1), protoVarintField(2, 1)...))),
		"overlong service charge":         replaceProtoField(t, fixture, 7, protoBytesField(7, []byte{0x10, 0x81, 0})),
		"scalar timestamp":                replaceProtoField(t, fixture, 2, protoVarintField(2, 100000)),
		"explicit zero nanos":             replaceProtoField(t, fixture, 2, protoBytesField(2, append(bytes.Clone(clockBytes), protoVarintField(2, 0)...))),
		"duplicate timestamp seconds":     replaceProtoField(t, fixture, 2, protoBytesField(2, append(bytes.Clone(clockBytes), clockBytes...))),
		"nested unknown":                  replaceProtoField(t, fixture, 9, protoBytesField(9, nestedUnknown)),
		"nested duplicate":                replaceProtoField(t, fixture, 9, protoBytesField(9, nestedDuplicate)),
		"noncanonical ready":              replaceProtoField(t, fixture, 9, protoBytesField(9, noncanonicalReady)),
		"explicit unknown kind":           replaceProtoField(t, fixture, 9, protoBytesField(9, explicitUnknown)),
		"overlong hash length":            replaceProtoField(t, fixture, 9, protoBytesField(9, overlongDigest)),
		"duplicate multihash":             replaceProtoField(t, fixture, 9, protoBytesField(9, duplicateMultihash)),
		"truncated":                       fixture[:len(fixture)-1],
		"JSON payload":                    []byte(`{"version":1}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseState(bad); err == nil {
				t.Fatal("accepted noncanonical snapshot")
			}
		})
	}
}

func TestSnapshotQueueAndRecordLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		size  int64
	}{
		{"pending", maxPending + 1, 1}, {"queued bytes", 3, 8 << 30}, {"records", maxRecords + 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newState(time.Unix(100000, 0))
			for i := range tc.count {
				d := publicationDigest(i, tc.size)
				s.Jobs[d] = &job{Digest: d, Created: s.Clock, Due: s.Clock, Kinds: 1}
				if tc.name == "records" {
					s.Jobs[d].Attempts = []peer.ID{publicationPeer(1), publicationPeer(2)}
					s.Jobs[d].Completed = []peer.ID{publicationPeer(1), publicationPeer(2)}
				}
			}
			encoded, err := marshalState(s)
			if err == nil {
				_, err = parseState(encoded)
			}
			if err == nil {
				t.Fatal("accepted state exceeding declared bounds")
			}
		})
	}
	if _, err := parseState(bytes.Repeat([]byte(" "), maxSnapshotBytes+1)); err == nil {
		t.Fatal("accepted oversized snapshot")
	}
}

func TestSnapshotBoundsNestedCollections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*publicationpb.Snapshot)
	}{
		{"jobs", func(s *publicationpb.Snapshot) {
			s.Jobs = make([]*publicationpb.Job, maxRecords+1)
			for i := range s.Jobs {
				s.Jobs[i] = &publicationpb.Job{}
			}
		}},
		{"associations", func(s *publicationpb.Snapshot) {
			s.Jobs[0].Associations = make([]*publicationpb.Association, 5)
			for i := range s.Jobs[0].Associations {
				s.Jobs[0].Associations[i] = &publicationpb.Association{Kind: publicationpb.WorkspaceKind(i), PartitionSha256: make([]byte, 32)}
			}
		}},
		{"service buckets", func(s *publicationpb.Snapshot) {
			s.Served = make([]*publicationpb.ServiceCredit, fairBuckets+1)
			for i := range s.Served {
				s.Served[i] = &publicationpb.ServiceCredit{Bucket: uint32(i), ChargedBytes: 1}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := snapshotProtoFixture(t)
			tc.mutate(value)
			if _, err := parseState(marshalProto(t, value)); err == nil {
				t.Fatal("accepted collection beyond decoder bound")
			}
		})
	}
}

func TestSnapshotProto3DefaultsAndFullWidthCounters(t *testing.T) {
	s := newState(time.Unix(0, 0))
	s.Expired = math.MaxUint64
	s.Served[0], s.Served[fairBuckets-1] = math.MaxUint64, math.MaxUint64
	d := cas.Sum(nil)
	if !s.observe(d, workspace.Association{Kind: workspace.Unknown}, false) {
		t.Fatal("empty blob observation rejected")
	}
	encoded, err := marshalState(s)
	if err != nil {
		t.Fatal(err)
	}
	var wire publicationpb.Snapshot
	if err := proto.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Version != 1 || len(wire.Served) != 2 || len(wire.Jobs) != 1 || len(wire.Jobs[0].Associations) != 1 {
		t.Fatal("default-valued state lost its explicit collection shape")
	}
	if wire.Clock == nil || wire.Window == nil || wire.NextStart == nil || wire.Jobs[0].Created == nil || wire.Jobs[0].Due == nil || !proto.Equal(wire.Clock, &timestamppb.Timestamp{}) {
		t.Fatal("epoch must retain present, empty timestamp messages")
	}
	if wire.Served[0].Bucket != 0 || wire.Served[1].Bucket != fairBuckets-1 || wire.Served[0].ChargedBytes != math.MaxUint64 || wire.Served[1].ChargedBytes != math.MaxUint64 {
		t.Fatal("sparse credits lost bucket identity or full-width values")
	}
	if !bytes.Equal(encoded, marshalProto(t, &wire)) {
		t.Fatal("encoder differs from deterministic generated protobuf")
	}
	parsed, err := parseState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	assertSameState(t, s, parsed)
}

func TestPublicationEpochProtobufGoldens(t *testing.T) {
	// Version and record kind are present; epoch timestamps are empty messages,
	// not absent fields. All other proto3 defaults are omitted.
	snapshotGolden, err := hex.DecodeString("080112001a003200")
	if err != nil {
		t.Fatal(err)
	}
	entryGolden, err := hex.DecodeString("08011208080112001a003200")
	if err != nil {
		t.Fatal(err)
	}
	s := newState(time.Unix(0, 0))
	encoded, err := marshalState(s)
	if err != nil || !bytes.Equal(encoded, snapshotGolden) {
		t.Fatalf("epoch snapshot differs from wire fixture: %x, %v", encoded, err)
	}
	restored, err := parseState(snapshotGolden)
	if err != nil {
		t.Fatal(err)
	}
	assertSameState(t, s, restored)
	expected := &publicationpb.Snapshot{
		Version: 1, Clock: &timestamppb.Timestamp{}, Window: &timestamppb.Timestamp{}, NextStart: &timestamppb.Timestamp{},
	}
	var snapshotMessage publicationpb.Snapshot
	if err := proto.Unmarshal(snapshotGolden, &snapshotMessage); err != nil || !proto.Equal(&snapshotMessage, expected) {
		t.Fatal("generated snapshot decoder changed required epoch timestamp presence")
	}
	frame, err := encodeEntry(s, true, nil)
	if err != nil || !bytes.Equal(frame[frameHeaderBytes:], entryGolden) {
		t.Fatalf("epoch checkpoint differs from wire fixture: %v", err)
	}
	restored, checkpoint, deleted, err := decodeEntry(entryGolden)
	if err != nil || !checkpoint || len(deleted) != 0 {
		t.Fatalf("epoch checkpoint failed to decode: %v", err)
	}
	assertSameState(t, s, restored)
	var entryMessage publicationpb.JournalEntry
	if err := proto.Unmarshal(entryGolden, &entryMessage); err != nil || !proto.Equal(&entryMessage, &publicationpb.JournalEntry{Kind: publicationpb.RecordKind_RECORD_KIND_CHECKPOINT, State: expected}) {
		t.Fatal("generated checkpoint decoder changed its typed structure")
	}
}

func TestSnapshotSparseServiceCredits(t *testing.T) {
	for _, count := range []int{0, fairBuckets} {
		s := newState(time.Unix(100000, 0))
		for i := range count {
			s.Served[i] = uint64(i + 1)
		}
		encoded, err := marshalState(s)
		if err != nil {
			t.Fatal(err)
		}
		var wire publicationpb.Snapshot
		if err := proto.Unmarshal(encoded, &wire); err != nil {
			t.Fatal(err)
		}
		if len(wire.Served) != count {
			t.Fatalf("encoded %d credits, want %d positive buckets", len(wire.Served), count)
		}
		parsed, err := parseState(encoded)
		if err != nil {
			t.Fatal(err)
		}
		assertSameState(t, s, parsed)
	}
}

func TestPropertySnapshotEncodingIgnoresMapInsertionOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		first, second := newState(time.Unix(100000, 0)), newState(time.Unix(100000, 0))
		count := rapid.IntRange(1, 24).Draw(t, "jobs")
		order := make([]int, count)
		for i := range order {
			order[i] = i
			first.observe(publicationDigest(i, int64(i+1)), association(byte(i), workspace.Canonical), i%2 == 0)
		}
		for i := range order {
			other := rapid.IntRange(0, count-1).Draw(t, "swap")
			order[i], order[other] = order[other], order[i]
		}
		for _, i := range order {
			second.observe(publicationDigest(i, int64(i+1)), association(byte(i), workspace.Canonical), i%2 == 0)
		}
		a, err := marshalState(first)
		if err != nil {
			t.Fatal(err)
		}
		b, err := marshalState(second)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("map insertion order changed snapshot")
		}
		parsed, err := parseState(a)
		if err != nil {
			t.Fatal(err)
		}
		b, err = marshalState(parsed)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("canonical snapshot did not round trip")
		}
	})
}

func TestPropertySnapshotPreservesSchedulingDecisions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := newState(time.Unix(100000, 0))
		self := publicationPeer(0)
		candidates := []peer.ID{publicationPeer(1), publicationPeer(2), publicationPeer(3)}
		for step := 0; step < 24; step++ {
			s.advance(time.Unix(s.Clock+int64(rapid.IntRange(-3600, 7200).Draw(t, "clock delta")), 0))
			id := rapid.IntRange(0, 15).Draw(t, "blob")
			d := publicationDigest(id, int64(1+id)*1024)
			a := association(byte(rapid.IntRange(0, 255).Draw(t, "partition")), workspace.Kind(rapid.IntRange(0, 3).Draw(t, "kind")))
			s.observe(d, a, rapid.Bool().Draw(t, "ready"))
			encoded, err := marshalState(s)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := parseState(encoded)
			if err != nil {
				t.Fatal(err)
			}
			policy := Policy(rapid.IntRange(1, 15).Draw(t, "policy"))
			originalAttempt := s.start(policy, self, candidates, 1<<20)
			restoredAttempt := restored.start(policy, self, candidates, 1<<20)
			if (originalAttempt == nil) != (restoredAttempt == nil) || originalAttempt != nil && *originalAttempt != *restoredAttempt {
				t.Fatal("snapshot changed the next peer or blob selection")
			}
			if originalAttempt != nil {
				complete := rapid.Bool().Draw(t, "complete")
				missing := !complete && rapid.Bool().Draw(t, "missing")
				s.finish(*originalAttempt, complete, missing)
				restored.finish(*restoredAttempt, complete, missing)
			}
			after, err := marshalState(s)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := marshalState(restored)
			if err != nil || !bytes.Equal(after, replayed) {
				t.Fatal("snapshot changed subsequent accounting or receipts")
			}
		}
	})
}

func TestOutboxLockAndInterruptedAttemptReopen(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(100000, 0)
	o, s, err := openOutbox(root, now)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if other, _, err := openOutbox(root, now); err == nil {
		_ = other.Close()
		t.Fatal("second writer obtained outbox lock")
	}
	d := publicationDigest(1, 1024)
	s.observe(d, association(1, workspace.Canonical), true)
	if s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 4096) == nil {
		t.Fatal("missing fixture attempt")
	}
	if err := o.Save(s); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	// A newer uninstalled temporary snapshot cannot replace committed state.
	if err := os.WriteFile(filepath.Join(root, "state.tmp"), []byte("interrupted replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, restored, err := openOutbox(root, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if restored.Bytes != 1024 || restored.Offers != 1 || len(restored.Jobs[d].Attempts) != 1 || restored.running != nil {
		t.Fatal("reopen lost interrupted attempt charge")
	}
	if _, err := os.Stat(filepath.Join(root, "state.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan temporary snapshot was retained")
	}
	restored.advance(time.Unix(1, 0))
	if restored.Bytes != s.Bytes || restored.Clock != s.Clock {
		t.Fatal("restart wall-clock rollback reset state")
	}
	if err := reopened.Save(restored); err != nil {
		t.Fatal(err)
	}
	assertStateBounds(t, restored, publicationPeer(0))
}

func TestOutboxRejectsCorruptionWithoutResetAndReleasesLock(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state.log")
	bad := []byte(`{"version":1,"jobs":"corrupted"}`)
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
		_ = o.Close()
		t.Fatal("corrupt snapshot silently reset")
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, bad) {
		t.Fatal("corrupt snapshot was overwritten")
	}
	if err := os.WriteFile(path, encodedJournalFixture(t), 0600); err != nil {
		t.Fatal(err)
	}
	o, _, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatalf("error leaked outbox lock: %v", err)
	}
	_ = o.Close()
}

type restoredIdentitySource struct{}

func (restoredIdentitySource) Has(context.Context, cas.Digest) (bool, error) { return true, nil }

type restoredIdentityTransport struct{ id peer.ID }

func (t restoredIdentityTransport) ID() peer.ID        { return t.id }
func (restoredIdentityTransport) SeedPeers() []peer.ID { return nil }
func (restoredIdentityTransport) Offer(context.Context, peer.ID, cas.Digest) (bool, error) {
	return false, nil
}

func TestRestoredReceiptsCannotCountCurrentProducer(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "state.log"), encodedJournalFixture(t), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := New(t.Context(), root, restoredIdentitySource{}, restoredIdentityTransport{id: publicationPeer(1)}, Options{Policy: 15})
	if err == nil {
		_ = p.Close()
		t.Fatal("restored producer identity counted as a remote receipt")
	}
	o, _, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatalf("identity rejection leaked snapshot lock: %v", err)
	}
	_ = o.Close()
}

func TestOutboxRejectsLinksAndUnexpectedEntries(t *testing.T) {
	for _, name := range []string{"root", "outbox.lock", "state.log", "state.tmp"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			root, outside := filepath.Join(parent, "outbox"), filepath.Join(parent, "outside")
			if err := os.Mkdir(outside, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(outside, "keep")
			contents := []byte("outside state must remain unchanged")
			if err := os.WriteFile(marker, contents, 0600); err != nil {
				t.Fatal(err)
			}
			link, target := root, outside
			if name != "root" {
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
				link, target = filepath.Join(root, name), marker
			}
			if err := os.Symlink(target, link); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				t.Fatal(err)
			}
			if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
				_ = o.Close()
				t.Fatal("followed a reserved symlink")
			}
			if got, err := os.ReadFile(marker); err != nil || !bytes.Equal(got, contents) {
				t.Fatal("wrote through reserved symlink")
			}
			if entries, err := os.ReadDir(outside); err != nil || len(entries) != 1 {
				t.Fatal("created files outside outbox")
			}
			if err := os.Remove(link); err != nil {
				t.Fatal(err)
			}
			o, _, err := openOutbox(root, time.Unix(100000, 0))
			if err != nil {
				t.Fatalf("link rejection leaked lock: %v", err)
			}
			_ = o.Close()
		})
	}
	root := t.TempDir()
	foreign := filepath.Join(root, "unrelated")
	if err := os.WriteFile(foreign, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
		_ = o.Close()
		t.Fatal("accepted unrelated files")
	}
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}
	o, _, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatal("unrelated file error leaked lock")
	}
	_ = o.Close()
}

func TestOutboxSaveRejectsReservedSymlinks(t *testing.T) {
	for _, name := range []string{"state.log", "state.tmp"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			o, s, err := openOutbox(root, time.Unix(100000, 0))
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			outside := filepath.Join(t.TempDir(), "keep")
			original := []byte("outside bytes")
			if err := os.WriteFile(outside, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(root, name)); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				t.Fatal(err)
			}
			if err := o.Save(s); err == nil {
				t.Fatal("save accepted reserved symlink")
			}
			if got, err := os.ReadFile(outside); err != nil || !bytes.Equal(got, original) {
				t.Fatal("save wrote through a symlink")
			}
		})
	}
}

func FuzzPublicationSnapshot(f *testing.F) {
	valid, err := marshalState(snapshotFixture())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`{"version":1}`))
	f.Add(valid[:len(valid)/2])
	f.Add(append(append([]byte{}, valid...), valid...))
	value := snapshotProtoFixture(f)
	value.Expired, value.Served[0].ChargedBytes = math.MaxUint64, math.MaxUint64
	f.Add(marshalProto(f, value))
	value.Jobs[0].Digest = nil
	f.Add(marshalProto(f, value))
	f.Add(replaceProtoField(f, valid, 1, []byte{8, 0x81, 0}))
	f.Add(replaceProtoField(f, valid, 9, bytes.Repeat(protoBytesField(9, nil), maxRecords+1)))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64<<10 {
			return
		}
		s, err := parseState(b)
		if err != nil {
			return
		}
		canonical, err := marshalState(s)
		if err != nil || !bytes.Equal(canonical, b) {
			t.Fatal("decoder accepted noncanonical state")
		}
		assertStateBounds(t, s, "")
	})
}
