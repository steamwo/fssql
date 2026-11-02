package stats

import "testing"

func TestRelationStatisticsValidation(t *testing.T) {
	if !(RelationStatistics{Source: Unknown, Confidence: 0}).Valid() {
		t.Fatal("unknown zero confidence should be valid")
	}
	if (RelationStatistics{Source: Unknown, Confidence: .5}).Valid() {
		t.Fatal("unknown must not carry confidence")
	}
	if (RelationStatistics{Source: StorageNative, Confidence: 1}).Valid() {
		t.Fatal("known source requires fingerprint")
	}
	if !(RelationStatistics{Source: Sampled, Confidence: .6, Fingerprint: "x", Rows: 42}).Valid() {
		t.Fatal("valid sampled stats rejected")
	}
}
