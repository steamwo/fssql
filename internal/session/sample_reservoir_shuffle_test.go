package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"
)

func TestDuckDBReservoirLibstdcppShuffle(t *testing.T) {
	var ref struct {
		Profile string
		Cases   []struct {
			Seed   uint64
			Count  int
			Next   uint32 `json:"next_uint32"`
			SHA256 string
		}
	}
	data, err := os.ReadFile("../../test/corpus/reservoir-shuffle-libstdcpp.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &ref); err != nil {
		t.Fatal(err)
	}
	if ref.Profile != "duckdb" {
		t.Fatal("wrong shuffle profile")
	}
	for _, tc := range ref.Cases {
		r := duckReservoir{rng: newSampleRandom(tc.Seed)}
		weights := make([]float64, tc.Count)
		for i := range weights {
			weights[i] = float64(i)
		}
		r.shuffleWeights(weights)
		h := sha256.New()
		for _, v := range weights {
			_, _ = h.Write([]byte(strconv.Itoa(int(v)) + "\n"))
		}
		if hex.EncodeToString(h.Sum(nil)) != tc.SHA256 || r.rng.next32() != tc.Next {
			t.Fatalf("libstdc++ shuffle/RNG consumption mismatch seed=%d count=%d", tc.Seed, tc.Count)
		}
	}
}
