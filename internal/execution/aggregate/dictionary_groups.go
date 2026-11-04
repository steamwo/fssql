package aggregate

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

const maxGroupDictionaryEntries = 512

// findDictionaryGroups hashes and compares each referenced dictionary entry
// once, then scatters its group ID. Unreferenced dictionary entries must never
// create empty groups, and distinct entries with equal values share a group.
func (a *HashAggregate) findDictionaryGroups(ctx context.Context, projected *vector.DataChunk, groupIDs []int) (bool, error) {
	if len(projected.Columns) != 1 {
		return false, nil
	}
	key := projected.Columns[0]
	child := key
	var selections [16]vector.SelectionVector
	depth := 0
	for {
		base, selection, dictionary := child.Dictionary()
		if !dictionary {
			break
		}
		if depth == len(selections) {
			return false, nil
		}
		selections[depth] = selection
		depth++
		child = base
	}
	if depth == 0 {
		return false, nil
	}
	entries := child.Count()
	if child.Kind == vector.Constant && entries > 0 {
		entries = 1
	}
	if entries > maxGroupDictionaryEntries {
		return false, nil
	}
	if projected.Cardinality > key.Count() {
		return true, errors.New("aggregate dictionary cardinality mismatch")
	}
	var entryToUnique [maxGroupDictionaryEntries]int
	for i := 0; i < entries; i++ {
		entryToUnique[i] = -1
	}
	var used [maxGroupDictionaryEntries]int
	unique := 0
	for row := 0; row < projected.Cardinality; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return true, err
			}
		}
		entry := row
		for i := 0; i < depth; i++ {
			if entry < 0 || entry >= selections[i].Len() {
				return true, errors.New("aggregate dictionary selection out of range")
			}
			entry = selections[i].Get(entry)
		}
		if child.Kind == vector.Constant {
			entry = 0
		}
		if entry < 0 || entry >= entries {
			return true, errors.New("aggregate dictionary index out of range")
		}
		if entryToUnique[entry] < 0 {
			entryToUnique[entry] = unique
			used[unique] = entry
			unique++
		}
		groupIDs[row] = entryToUnique[entry]
	}
	if unique*2 > projected.Cardinality {
		return false, nil
	}
	uniqueKeys, err := vector.NewDictionary(child, vector.SelectionFromRows(used[:unique]))
	if err != nil {
		return true, err
	}
	defer uniqueKeys.Release() // borrows child; input owns its lifetime
	keys := &vector.DataChunk{Columns: []*vector.Vector{uniqueKeys}, Cardinality: unique}
	var hashes [maxGroupDictionaryEntries]uint64
	if err := exechash.HashChunk(keys, a.groupKeys, hashes[:unique]); err != nil {
		return true, err
	}
	var groups [maxGroupDictionaryEntries]int
	if err := a.findOrCreateGroups(ctx, keys, hashes[:unique], groups[:unique]); err != nil {
		return true, err
	}
	for row := range groupIDs {
		groupIDs[row] = groups[groupIDs[row]]
	}
	return true, nil
}
