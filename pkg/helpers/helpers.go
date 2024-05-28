package helpers

import (
	"fmt"
	"strings"
	"time"

	"github.com/backdrop-run/polystore/pkg/types"
)

// GetObjectSliceDiff takes two objects slices and returns an ObjectSliceDiff
func GetObjectSliceDiff(prev []types.Object, curr []types.Object, timestampTolerance time.Duration) types.ObjectSliceDiff {
	var diff types.ObjectSliceDiff
	pos := make(map[string]types.Object)
	cos := make(map[string]types.Object)
	for _, o := range prev {
		pos[o.Path] = o
	}
	for _, o := range curr {
		cos[o.Path] = o
	}
	// for every object in the previous slice, if it exists in the current slice, check if it is *considered as* updated;
	// otherwise, mark it as removed
	for _, p := range prev {
		if c, found := cos[p.Path]; found {
			if c.LastModified.Sub(p.LastModified) > timestampTolerance {
				diff.Updated = append(diff.Updated, c)
			}
		} else {
			diff.Removed = append(diff.Removed, p)
		}
	}
	// for every object in the current slice, if it does not exist in the previous slice, mark it as added
	for _, c := range curr {
		if _, found := pos[c.Path]; !found {
			diff.Added = append(diff.Added, c)
		}
	}
	// if any object is marked as removed or added or updated, set change to true
	diff.Change = len(diff.Removed)+len(diff.Added)+len(diff.Updated) > 0
	return diff
}

func CleanPrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}

func RemovePrefixFromObjectPath(prefix string, path string) string {
	if prefix == "" {
		return path
	}
	path = strings.Replace(path, fmt.Sprintf("%s/", prefix), "", 1)
	return path
}

func ObjectPathIsInvalid(path string) bool {
	return strings.Contains(path, "/") || path == ""
}
