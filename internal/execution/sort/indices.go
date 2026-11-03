package sort

import "math/bits"

// Three-way introsort keeps duplicate keys cheap, uses no row allocations,
// and falls back to heapsort at a bounded recursion depth.
func sortManagedIndices(idx *managedIndices, compare func(int, int) (int, error)) error {
	var order func(int, int, int) error
	heap := func(lo, hi int) error {
		n := hi - lo
		sift := func(root, end int) error {
			for child := root*2 + 1; child < end; child = root*2 + 1 {
				if child+1 < end {
					c, err := compare(idx.get(lo+child), idx.get(lo+child+1))
					if err != nil {
						return err
					}
					if c < 0 {
						child++
					}
				}
				c, err := compare(idx.get(lo+root), idx.get(lo+child))
				if err != nil {
					return err
				}
				if c >= 0 {
					return nil
				}
				idx.swap(lo+root, lo+child)
				root = child
			}
			return nil
		}
		for root := n/2 - 1; root >= 0; root-- {
			if err := sift(root, n); err != nil {
				return err
			}
		}
		for end := n - 1; end > 0; end-- {
			idx.swap(lo, lo+end)
			if err := sift(0, end); err != nil {
				return err
			}
		}
		return nil
	}
	order = func(lo, hi, depth int) error {
		for hi-lo > 16 {
			if depth == 0 {
				return heap(lo, hi)
			}
			depth--
			mid := lo + (hi-lo)/2
			for _, pair := range [3][2]int{{lo, mid}, {mid, hi - 1}, {lo, mid}} {
				c, err := compare(idx.get(pair[0]), idx.get(pair[1]))
				if err != nil {
					return err
				}
				if c > 0 {
					idx.swap(pair[0], pair[1])
				}
			}
			pivot := idx.get(mid)
			left, current, right := lo, lo, hi
			for current < right {
				c, err := compare(idx.get(current), pivot)
				if err != nil {
					return err
				}
				if c < 0 {
					idx.swap(left, current)
					left++
					current++
				} else if c > 0 {
					right--
					idx.swap(current, right)
				} else {
					current++
				}
			}
			if left-lo < hi-right {
				if err := order(lo, left, depth); err != nil {
					return err
				}
				lo = right
			} else {
				if err := order(right, hi, depth); err != nil {
					return err
				}
				hi = left
			}
		}
		for i := lo + 1; i < hi; i++ {
			for j := i; j > lo; j-- {
				c, err := compare(idx.get(j), idx.get(j-1))
				if err != nil {
					return err
				}
				if c >= 0 {
					break
				}
				idx.swap(j, j-1)
			}
		}
		return nil
	}
	return order(0, idx.n, 2*bits.Len(uint(idx.n)))
}
