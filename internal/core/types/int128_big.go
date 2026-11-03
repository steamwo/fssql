package types

import "math/big"

var (
	int128Min = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	int128Max = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	int128Mod = new(big.Int).Lsh(big.NewInt(1), 128)
)

// Int128FromBig converts one exact integer to the canonical signed Int128
// representation. The input is never mutated.
func Int128FromBig(value *big.Int) (Int128, bool) {
	if value == nil || value.Cmp(int128Min) < 0 || value.Cmp(int128Max) > 0 {
		return Int128{}, false
	}
	if value.IsInt64() {
		return Int128FromInt64(value.Int64()), true
	}
	unsigned := new(big.Int).Set(value)
	if unsigned.Sign() < 0 {
		unsigned.Add(unsigned, int128Mod)
	}
	lo := new(big.Int).And(new(big.Int).Set(unsigned), new(big.Int).SetUint64(^uint64(0))).Uint64()
	hi := new(big.Int).Rsh(unsigned, 64).Uint64()
	return Int128{Lo: lo, Hi: int64(hi)}, true
}
