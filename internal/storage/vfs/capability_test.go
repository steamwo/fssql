package vfs

import "testing"

func TestCapabilitySetOperations(t *testing.T) {
	set := NewCapabilitySet(CapabilityRandomRead, CapabilityStreamingRead)

	if !set.Has(CapabilityRandomRead) {
		t.Fatal("RandomRead should be present")
	}
	if set.Has(CapabilityParallelRandomRead) {
		t.Fatal("ParallelRandomRead should not be present")
	}

	set = set.With(CapabilityParallelRandomRead)
	if !set.Has(CapabilityParallelRandomRead) {
		t.Fatal("With should add capability")
	}

	set = set.Without(CapabilityStreamingRead)
	if set.Has(CapabilityStreamingRead) {
		t.Fatal("Without should remove capability")
	}

	other := NewCapabilitySet(CapabilityRandomRead, CapabilityServerSideList)
	intersection := set.Intersect(other)
	if !intersection.Has(CapabilityRandomRead) || intersection.Has(CapabilityParallelRandomRead) || intersection.Has(CapabilityServerSideList) {
		t.Fatalf("unexpected intersection: %#x", uint64(intersection))
	}
}

func TestAtomicPublishWriteCapabilityIsIndependentFromAtomicRename(t *testing.T) {
	set := Capabilities(AtomicRename)
	if set.Has(AtomicPublishWrite) {
		t.Fatal("AtomicRename must not imply AtomicPublishWrite")
	}
	set = set.With(AtomicPublishWrite)
	if !set.Has(AtomicPublishWrite) || !set.Has(AtomicRename) {
		t.Fatalf("unexpected capability set: %#x", uint64(set))
	}
}

func TestResolveEffectiveCapabilitiesRuntimeProbeOnlyNarrows(t *testing.T) {
	static := NewCapabilitySet(
		CapabilityRandomRead,
		CapabilityParallelRandomRead,
		CapabilityStreamingRead,
	)
	probe := ProbeResult{
		Known: NewCapabilitySet(
			CapabilityParallelRandomRead,
			CapabilityServerSideList,
		),
		Supported: NewCapabilitySet(CapabilityServerSideList),
	}

	effective, err := ResolveEffectiveCapabilities(static, probe)
	if err != nil {
		t.Fatalf("ResolveEffectiveCapabilities() error = %v", err)
	}
	if !effective.Has(CapabilityRandomRead) || !effective.Has(CapabilityStreamingRead) {
		t.Fatalf("unprobed static capabilities should remain: %#x", uint64(effective))
	}
	if effective.Has(CapabilityParallelRandomRead) {
		t.Fatal("known unsupported capability should be removed")
	}
	if effective.Has(CapabilityServerSideList) {
		t.Fatal("runtime probe must not grant capability absent from static driver set")
	}
}

func TestResolveEffectiveCapabilitiesRejectsInconsistentProbe(t *testing.T) {
	_, err := ResolveEffectiveCapabilities(
		NewCapabilitySet(CapabilityRandomRead),
		ProbeResult{Supported: NewCapabilitySet(CapabilityRandomRead)},
	)
	if err == nil {
		t.Fatal("expected inconsistent probe to fail validation")
	}
}

func TestParseFallbackPolicy(t *testing.T) {
	tests := []struct {
		input string
		want  FallbackPolicy
	}{
		{input: "deny", want: FallbackDeny},
		{input: "warn", want: FallbackWarn},
		{input: "allow", want: FallbackAllow},
	}

	for _, test := range tests {
		got, err := ParseFallbackPolicy(test.input)
		if err != nil {
			t.Fatalf("ParseFallbackPolicy(%q) error = %v", test.input, err)
		}
		if got != test.want {
			t.Fatalf("ParseFallbackPolicy(%q) = %v, want %v", test.input, got, test.want)
		}
	}

	if _, err := ParseFallbackPolicy("silent"); err == nil {
		t.Fatal("invalid policy should fail")
	}
}

func TestWholeFileRangeFallbackDecisionIsNeverImplicit(t *testing.T) {
	tests := []struct {
		policy FallbackPolicy
		want   FallbackDecision
	}{
		{policy: FallbackDeny, want: FallbackDecision{}},
		{policy: FallbackWarn, want: FallbackDecision{Allowed: true, Warn: true}},
		{policy: FallbackAllow, want: FallbackDecision{Allowed: true}},
		{policy: FallbackPolicy(255), want: FallbackDecision{}},
	}

	for _, test := range tests {
		if got := WholeFileRangeFallbackDecision(test.policy); got != test.want {
			t.Fatalf("WholeFileRangeFallbackDecision(%v) = %+v, want %+v", test.policy, got, test.want)
		}
	}
}
