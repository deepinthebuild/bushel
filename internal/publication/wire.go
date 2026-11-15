package publication

import (
	"errors"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

type wireMessage uint8

const (
	digestWire wireMessage = iota + 1
	timestampWire
	peerIDWire
	associationWire
	serviceCreditWire
	jobWire
	snapshotWire
	entryWire
)

// These bounds mirror journal.proto. Preflight scans without building message
// objects, preventing a small payload with millions of empty submessages from
// making generated unmarshalling allocate far beyond the journal's byte limit.
// Canonical field order/default omission and minimal varints are checked here,
// so recovery does not need to re-encode messages to validate their encoding.
type wireField struct {
	kind  protowire.Type
	count int
	bytes int
	value uint64
	child wireMessage
}

var digestFields = [...]wireField{
	{},
	{kind: protowire.BytesType, count: 1, bytes: 32},
	{kind: protowire.VarintType, count: 1, value: math.MaxUint64},
}

var timestampFields = [...]wireField{
	{},
	{kind: protowire.VarintType, count: 1, value: math.MaxInt64},
	// Publication accounting uses whole seconds; nanos must be omitted.
	{kind: protowire.VarintType, count: 1},
}

var peerIDFields = [...]wireField{
	{},
	{kind: protowire.BytesType, count: 1, bytes: 128},
}

var associationFields = [...]wireField{
	{},
	{kind: protowire.VarintType, count: 1, value: 3},
	{kind: protowire.BytesType, count: 1, bytes: 32},
}

var serviceCreditFields = [...]wireField{
	{},
	{kind: protowire.VarintType, count: 1, value: fairBuckets - 1},
	{kind: protowire.VarintType, count: 1, value: math.MaxUint64},
}

var jobFields = [...]wireField{
	{},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: digestWire},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: timestampWire},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: timestampWire},
	{kind: protowire.VarintType, count: 1, value: 1},
	{kind: protowire.BytesType, count: 4, bytes: maxSnapshotBytes, child: associationWire},
	{kind: protowire.BytesType, count: maxAttempts, bytes: maxSnapshotBytes, child: peerIDWire},
	{kind: protowire.BytesType, count: ReplicaTarget, bytes: maxSnapshotBytes, child: peerIDWire},
}

var snapshotFields = [...]wireField{
	{},
	{kind: protowire.VarintType, count: 1, value: math.MaxUint32},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: timestampWire},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: timestampWire},
	{kind: protowire.VarintType, count: 1, value: math.MaxUint64},
	{kind: protowire.VarintType, count: 1, value: math.MaxUint32},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: timestampWire},
	{kind: protowire.BytesType, count: fairBuckets, bytes: maxSnapshotBytes, child: serviceCreditWire},
	{kind: protowire.VarintType, count: 1, value: math.MaxUint64},
	{kind: protowire.BytesType, count: maxRecords, bytes: maxSnapshotBytes, child: jobWire},
}

var entryFields = [...]wireField{
	{},
	{kind: protowire.VarintType, count: 1, value: 2},
	{kind: protowire.BytesType, count: 1, bytes: maxSnapshotBytes, child: snapshotWire},
	{kind: protowire.BytesType, count: maxRecords, bytes: maxSnapshotBytes, child: digestWire},
}

func checkProtoBounds(b []byte, message wireMessage) error {
	var fields []wireField
	switch message {
	case digestWire:
		fields = digestFields[:]
	case timestampWire:
		fields = timestampFields[:]
	case peerIDWire:
		fields = peerIDFields[:]
	case associationWire:
		fields = associationFields[:]
	case serviceCreditWire:
		fields = serviceCreditFields[:]
	case jobWire:
		fields = jobFields[:]
	case snapshotWire:
		fields = snapshotFields[:]
	case entryWire:
		fields = entryFields[:]
	default:
		return errors.New("invalid publication message schema")
	}
	var counts [len(snapshotFields)]int
	var previous protowire.Number
	for len(b) != 0 {
		number, kind, n := protowire.ConsumeTag(b)
		if n < 0 || number <= 0 || int64(number) >= int64(len(fields)) || number < previous || n != protowire.SizeTag(number) {
			return errors.New("invalid publication protobuf field")
		}
		field := fields[number]
		counts[number]++
		if kind != field.kind || counts[number] > field.count {
			return errors.New("publication protobuf field exceeds bounds")
		}
		previous = number
		b = b[n:]
		if kind == protowire.VarintType {
			value, n := protowire.ConsumeVarint(b)
			if n < 0 || value == 0 || value > field.value || n != protowire.SizeVarint(value) {
				return errors.New("noncanonical publication protobuf integer")
			}
			b = b[n:]
			continue
		}
		value, n := protowire.ConsumeBytes(b)
		if n < 0 || len(value) == 0 && field.child != timestampWire || len(value) > field.bytes || n-len(value) != protowire.SizeVarint(uint64(len(value))) {
			return errors.New("invalid publication protobuf length")
		}
		b = b[n:]
		if field.child != 0 {
			if err := checkProtoBounds(value, field.child); err != nil {
				return err
			}
		}
	}
	return nil
}
