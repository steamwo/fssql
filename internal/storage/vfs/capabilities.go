package vfs

import "fmt"

// Capability identifies an operation a VFS driver can safely provide.
type Capability uint16

const (
	RandomRead Capability = iota
	ParallelRandomRead
	StreamingRead
	StreamingWrite
	RandomWrite
	MultipartWrite
	AtomicRename
	ConditionalRead
	ConditionalWrite
	StableETag
	VersionedObject
	ServerSideList
)

type CapabilitySet uint64

func Capabilities(cs ...Capability) CapabilitySet {
	var set CapabilitySet
	for _, c := range cs {
		set |= CapabilitySet(1) << c
	}
	return set
}

func (s CapabilitySet) Has(c Capability) bool { return s&(CapabilitySet(1)<<c) != 0 }
func (s CapabilitySet) Intersect(other CapabilitySet) CapabilitySet { return s & other }

type ProbeResult struct {
	Supported CapabilitySet
	Endpoint string
	CredentialScope string
	CredentialVersion string
	SecurityProfile string
}

func (p ProbeResult) Validate() error {
	if p.Endpoint == "" {
		return fmt.Errorf("vfs: probe missing endpoint")
	}
	if p.CredentialScope == "" {
		return fmt.Errorf("vfs: probe missing credential scope")
	}
	if p.SecurityProfile == "" {
		return fmt.Errorf("vfs: probe missing security profile")
	}
	return nil
}

func ResolveEffectiveCapabilities(static CapabilitySet, probe ProbeResult) (CapabilitySet, error) {
	if err := probe.Validate(); err != nil {
		return 0, err
	}
	return static.Intersect(probe.Supported), nil
}
