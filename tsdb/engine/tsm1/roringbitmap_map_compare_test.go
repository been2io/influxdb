package tsm1_test

import (
	"github.com/influxdata/roaring"
	"testing"
	"time"
)

type search interface {
	offset(i int) int32
	set(i int, off int32)
}
type roaringSearch struct {
	offsets []int32
	bm      *roaring.Bitmap
}

func (rs *roaringSearch) offset(i int) int32 {
	idx := rs.bm.Rank(uint32(i)) - 1
	return rs.offsets[idx]
}
func (rs *roaringSearch) set(i int, off int32) {
	rs.bm.Add(uint32(i))
	idx := rs.bm.Rank(uint32(i)) - 1
	rs.offsets[idx] = off
}

var leng = 50000000
var roar = &roaringSearch{
	offsets: make([]int32, leng),
	bm:      roaring.New(),
}

func initRoaring() {
	for i := 0; i < leng; i++ {
		roar.set(i, int32(i))
	}
}

func TestRoaringbitmap(t *testing.T) {
	initRoaring()
	now := time.Now()
	var a int32
	for i := 0; i < leng; i++ {
		a = roar.offset(i)
	}
	println(a)
	println(time.Now().Sub(now).String())
}
func TestMarkHashmap(t *testing.T) {
	m := make(map[int]int32)
	for i := 0; i < leng; i++ {
		m[i] = int32(i)
	}
	now := time.Now()
	var a int32
	for i := 0; i < leng; i++ {

		a = m[i]
	}
	println(a)
	println(time.Now().Sub(now).String())
}
func TestMarkDivideSearch(t *testing.T) {
	var m = make([]int32, leng)
	var m2 = make([]int32, leng)
	for i := 0; i < leng; i++ {
		m[i] = int32(i)
		m2[i] = int32(i)
	}
	now := time.Now()
	var a int32
	for i := 0; i < leng; i++ {
		i2 := int32(i)
		a1 := int32(binary_search2(m, i2))
		a = m2[a1]
	}
	println(a)
	println(time.Now().Sub(now).String())
}
func binary_search2(arr []int32, key int32) int {
	lo, hi := 0, len(arr)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if arr[mid] < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	if lo == hi && arr[lo] == key {
		return lo
	} else {
		return -(lo + 1)
	}
}
