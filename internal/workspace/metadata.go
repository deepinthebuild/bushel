// Package workspace classifies bounded, untrusted Bazel request metadata.
package workspace

import (
	"bytes"
	"context"
	"unicode"
	"unicode/utf8"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	MetadataHeader   = "build.bazel.remote.execution.v2.requestmetadata-bin"
	MaxMetadataBytes = 8 << 10
	MaxTargetBytes   = 4 << 10
	MaxIDBytes       = 256
)

// Metadata contains only local classification/accounting inputs. Valid is false
// for absent, conflicting, malformed, or oversized headers; those requests still
// perform ordinary CAS operations. A valid envelope may have an invalid label.
type Metadata struct {
	Valid                   bool
	InvocationID            string
	ActionID                string
	TargetID                string
	CorrelatedInvocationsID string
}

func FromContext(ctx context.Context) Metadata {
	return ParseMetadata(metadata.ValueFromIncomingContext(ctx, MetadataHeader))
}

// ParseMetadata is independent of gRPC transport state. Repeated equal headers
// coalesce; different protobuf messages never silently override one another.
func ParseMetadata(headers []string) Metadata {
	if len(headers) == 0 || len(headers) > MaxMetadataBytes/len(MetadataHeader) {
		return Metadata{}
	}
	var size int
	for _, value := range headers {
		// Include repeated header names so empty values cannot turn a small
		// byte limit into thousands of protobuf decodes.
		size += len(MetadataHeader)
		if len(value) > MaxMetadataBytes-size {
			return Metadata{}
		}
		size += len(value)
	}
	var first *repb.RequestMetadata
	for _, value := range headers {
		raw := []byte(value)
		if !unambiguousFields(raw) {
			return Metadata{}
		}
		message := &repb.RequestMetadata{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw, message); err != nil {
			return Metadata{}
		}
		if first != nil && !proto.Equal(first, message) {
			return Metadata{}
		}
		first = message
	}
	if !boundedText(first.ConfigurationId, MaxIDBytes) {
		return Metadata{}
	}
	m := Metadata{Valid: true, InvocationID: first.ToolInvocationId, ActionID: first.ActionId,
		TargetID: first.TargetId, CorrelatedInvocationsID: first.CorrelatedInvocationsId}
	if !m.validFields() {
		return Metadata{}
	}
	return m
}

// Protobuf normally lets the last singular field win. Classification must not
// infer from an absent target when a malformed target field was explicitly sent,
// or choose one of several conflicting singular values within a single header.
func unambiguousFields(raw []byte) bool {
	var fields [8][]byte
	var seen [8]bool
	for len(raw) > 0 {
		number, kind, n := protowire.ConsumeTag(raw)
		if n < 0 || kind == protowire.StartGroupType || kind == protowire.EndGroupType {
			return false
		}
		raw = raw[n:]
		n = protowire.ConsumeFieldValue(number, kind, raw)
		if n < 0 {
			return false
		}
		if number >= 1 && number <= 7 {
			if kind != protowire.BytesType || seen[number] && !bytes.Equal(fields[number], raw[:n]) {
				return false
			}
			fields[number], seen[number] = raw[:n], true
		}
		raw = raw[n:]
	}
	return true
}

func (m Metadata) validFields() bool {
	return m.Valid && boundedText(m.InvocationID, MaxIDBytes) && boundedText(m.ActionID, MaxIDBytes) &&
		boundedText(m.CorrelatedInvocationsID, MaxIDBytes) && boundedText(m.TargetID, MaxTargetBytes)
}

func boundedText(value string, maximum int) bool {
	if len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
