package vfs

import "fmt"

// Capability describes one operation or consistency guarantee exposed by a VFS driver.
// Capabilities are intentionally format-agnostic; VFS must not know about Parquet/CSV/JSON.
type Capability uint64

const (
	CapabilityRandomRead Capability = 1 << iota
	CapabilityParallelRandomRead
	CapabilityStreamingRead
	CapabilityStreamingWrite
	CapabilityRandomWrite
	CapabilityMultipartWrite
	CapabilityAtomicRename
	CapabilityConditionalRead
	CapabilityConditionalWrite
	CapabilityStableETag
	CapabilityVersionedObject
	CapabilityServerSideList
)

// CapabilitySet is a compact immutable-style bitset used to advertise VFS capabilities.
type CapabilitySet uint64

// NewCapabilitySet builds a capability set from individual capabilities.
func NewCapabilitySet(capabilities ...Capability) CapabilitySet {
	var set CapabilitySet
	for _, capability := range capabilities {
		set |= CapabilitySet(capability)
	}
	return set
}

// Has reports whether every bit in capability is present in the set.
func (set CapabilitySet) Has(capability Capability) bool {
	return capability != 0 && set&CapabilitySet(capability) == CapabilitySet(capability)
}

// With returns a set containing capability in addition to the current set.
func (set CapabilitySet) With(capability Capability) CapabilitySet {
	return set | CapabilitySet(capability)
}

// Without returns a set with capability removed.
func (set CapabilitySet) Without(capability Capability) CapabilitySet {
	return set &^ CapabilitySet(capability)
}

// Intersect returns the capabilities present in both sets.
func (set CapabilitySet) Intersect(other CapabilitySet) CapabilitySet {
	return set & other
}

// ProbeResult captures endpoint-specific runtime capability knowledge.
// Known identifies capabilities that were actually probed. Supported is a subset of Known.
// Capabilities outside Known retain their static driver value.
type ProbeResult struct {
	Known     CapabilitySet
	Supported CapabilitySet
}

// Validate checks the internal consistency of a probe result.
func (result ProbeResult) Validate() error {
	if result.Supported&^result.Known != 0 {
		return fmt.Errorf("vfs: probe marks unsupported-as-known capability bits: supported=%#x known=%#x", uint64(result.Supported), uint64(result.Known))
	}
	return nil
}

// ResolveEffectiveCapabilities combines driver capabilities with an endpoint runtime probe.
// Runtime probing may remove a statically advertised capability, but can never grant a
// capability the driver itself does not implement.
func ResolveEffectiveCapabilities(static CapabilitySet, probe ProbeResult) (CapabilitySet, error) {
	if err := probe.Validate(); err != nil {
		return 0, err
	}
	unsupported := probe.Known &^ probe.Supported
	return static &^ unsupported, nil
}

// FallbackPolicy controls explicit remote fallbacks when a requested capability is unavailable.
type FallbackPolicy uint8

const (
	FallbackDeny FallbackPolicy = iota
	FallbackWarn
	FallbackAllow
)

// ParseFallbackPolicy parses the documented remote_fallback_policy values.
func ParseFallbackPolicy(value string) (FallbackPolicy, error) {
	switch value {
	case "deny":
		return FallbackDeny, nil
	case "warn":
		return FallbackWarn, nil
	case "allow":
		return FallbackAllow, nil
	default:
		return FallbackDeny, fmt.Errorf("vfs: invalid remote fallback policy %q", value)
	}
}

// FallbackDecision is the explicit result of evaluating a fallback policy.
// A whole-file range fallback must never happen without consulting this decision.
type FallbackDecision struct {
	Allowed bool
	Warn    bool
}

// WholeFileRangeFallbackDecision evaluates whether a whole-file range fallback is permitted.
func WholeFileRangeFallbackDecision(policy FallbackPolicy) FallbackDecision {
	switch policy {
	case FallbackWarn:
		return FallbackDecision{Allowed: true, Warn: true}
	case FallbackAllow:
		return FallbackDecision{Allowed: true}
	default:
		return FallbackDecision{}
	}
}
