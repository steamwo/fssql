package types

import (
	"fmt"
	"math/big"
	"strings"
)

// ParseDecimalText parses an exact base-10 DECIMAL value into the canonical
// unscaled Int128 representation. Extra fractional zeroes are accepted; any
// non-zero precision beyond scale is rejected instead of rounded.
func ParseDecimalText(input string, width, scale uint8) (Int128, error) {
	if width == 0 || width > 38 || scale > width { return Int128{}, fmt.Errorf("invalid DECIMAL(%d,%d)", width, scale) }
	text := strings.TrimSpace(input)
	if text == "" { return Int128{}, fmt.Errorf("empty DECIMAL value") }
	negative := false
	if text[0] == '+' || text[0] == '-' { negative = text[0] == '-'; text = text[1:] }
	if text == "" || strings.Count(text, ".") > 1 { return Int128{}, fmt.Errorf("invalid DECIMAL %q", input) }
	parts := strings.SplitN(text, ".", 2); whole := parts[0]; fraction := ""
	if len(parts) == 2 { fraction = parts[1] }
	if whole == "" { whole = "0" }
	if !decimalTextDigits(whole) || (fraction != "" && !decimalTextDigits(fraction)) { return Int128{}, fmt.Errorf("invalid DECIMAL %q", input) }
	for len(fraction) > int(scale) { if fraction[len(fraction)-1] != '0' { return Int128{}, fmt.Errorf("DECIMAL %q cannot be represented exactly at scale %d", input, scale) }; fraction = fraction[:len(fraction)-1] }
	for len(fraction) < int(scale) { fraction += "0" }
	digits := strings.TrimLeft(whole+fraction, "0"); if digits == "" { digits = "0" }
	if len(digits) > int(width) { return Int128{}, fmt.Errorf("DECIMAL %q exceeds precision %d", input, width) }
	value := new(big.Int); if _, ok := value.SetString(digits, 10); !ok { return Int128{}, fmt.Errorf("invalid DECIMAL %q", input) }
	if negative && value.Sign() != 0 { value.Neg(value) }
	out, ok := Int128FromBig(value); if !ok { return Int128{}, fmt.Errorf("DECIMAL %q exceeds Int128 storage", input) }
	return out, nil
}

// Int128Big returns the exact signed big.Int represented by v.
func Int128Big(v Int128) *big.Int {
	hi := new(big.Int).SetUint64(uint64(v.Hi)); hi.Lsh(hi, 64); hi.Or(hi, new(big.Int).SetUint64(v.Lo))
	if v.Hi < 0 { hi.Sub(hi, new(big.Int).Lsh(big.NewInt(1), 128)) }
	return hi
}

// FormatDecimalText formats an unscaled DECIMAL without floating conversion.
func FormatDecimalText(v Int128, scale uint8) string {
	n := Int128Big(v); negative := n.Sign() < 0; if negative { n.Neg(n) }
	digits := n.String()
	if scale == 0 { if negative && digits != "0" { return "-"+digits }; return digits }
	for len(digits) <= int(scale) { digits = "0"+digits }
	cut := len(digits)-int(scale); out := digits[:cut]+"."+digits[cut:]
	if negative && n.Sign() != 0 { out = "-"+out }
	return out
}

func decimalTextDigits(s string) bool { if s=="" { return false }; for _,r:=range s { if r<'0'||r>'9' { return false } }; return true }
