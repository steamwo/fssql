package semantics

import (
	"errors"
	"math"
)

var ErrOverflow = errors.New("numeric overflow")

const nullHashSeed uint64 = 0x9ae16a3b2f90404f

func EqualInt64(a, b int64) bool { return a == b }
func CompareInt64(a, b int64) int {
	if a < b { return -1 }
	if a > b { return 1 }
	return 0
}

// DuckDB-style grouping/order semantics canonicalize NaNs as equal keys and
// order NaN after finite floating-point values. Signed zero remains equal.
func EqualFloat32(a, b float32) bool { return a == b || (math.IsNaN(float64(a)) && math.IsNaN(float64(b))) }
func EqualFloat64(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }
func CompareFloat32(a, b float32) int {
	an, bn := math.IsNaN(float64(a)), math.IsNaN(float64(b))
	if an { if bn { return 0 }; return 1 }
	if bn { return -1 }
	if a < b { return -1 }
	if a > b { return 1 }
	return 0
}
func CompareFloat64(a, b float64) int {
	an, bn := math.IsNaN(a), math.IsNaN(b)
	if an { if bn { return 0 }; return 1 }
	if bn { return -1 }
	if a < b { return -1 }
	if a > b { return 1 }
	return 0
}

func HashNull() uint64           { return mix64(nullHashSeed) }
func HashInt64(v int64) uint64   { return mix64(uint64(v)) }
func HashUint64(v uint64) uint64 { return mix64(v) }
func HashBool(v bool) uint64 { if v { return mix64(1) }; return mix64(0) }
func HashFloat32(v float32) uint64 { bits:=math.Float32bits(v);if v==0{bits=0};if math.IsNaN(float64(v)){bits=0x7fc00000};return mix64(uint64(bits)) }
func HashFloat64(v float64) uint64 { bits:=math.Float64bits(v);if v==0{bits=0};if math.IsNaN(v){bits=0x7ff8000000000000};return mix64(bits) }
func HashInt128(lo uint64, hi int64) uint64 { return CombineHash(HashUint64(lo), HashInt64(hi)) }
func HashString(s string) uint64 { const offset=uint64(14695981039346656037);const prime=uint64(1099511628211);h:=offset;for i:=0;i<len(s);i++{h=(h^uint64(s[i]))*prime};return mix64(h) }
func HashBytes(b []byte) uint64 { const offset=uint64(14695981039346656037);const prime=uint64(1099511628211);h:=offset;for _,c:=range b{h=(h^uint64(c))*prime};return mix64(h) }
func CombineHash(seed,value uint64)uint64{return mix64(seed^(value+0x9e3779b97f4a7c15+(seed<<6)+(seed>>2)))}
func mix64(x uint64)uint64{x^=x>>30;x*=0xbf58476d1ce4e5b9;x^=x>>27;x*=0x94d049bb133111eb;x^=x>>31;return x}

func AddInt64(a,b int64)(int64,error){c:=a+b;if(b>0&&c<a)||(b<0&&c>a){return 0,ErrOverflow};return c,nil}
func SubInt64(a,b int64)(int64,error){if b==math.MinInt64{if a>=0{return 0,ErrOverflow};return a-b,nil};return AddInt64(a,-b)}
func MulInt64(a,b int64)(int64,error){if a==0||b==0{return 0,nil};if a==math.MinInt64&&b==-1||b==math.MinInt64&&a==-1{return 0,ErrOverflow};c:=a*b;if c/b!=a{return 0,ErrOverflow};return c,nil}
