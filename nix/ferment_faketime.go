// Added to Go's time package by ferment (nix/fakeable-go.nix).

package time

import (
	"sync"
	"syscall"
)

var (
	fermentOnce   sync.Once
	fermentOffset int64
)

// fermentFakeOffset reads FAKETIME once: "+N" or "-N" whole seconds (the
// libfaketime offset form ferment uses). Any other value means no offset.
func fermentFakeOffset() int64 {
	fermentOnce.Do(func() {
		v, ok := syscall.Getenv("FAKETIME")
		if !ok || len(v) < 2 || (v[0] != '+' && v[0] != '-') {
			return
		}
		var n int64
		for _, c := range v[1:] {
			if c < '0' || c > '9' {
				return
			}
			n = n*10 + int64(c-'0')
		}
		if v[0] == '-' {
			n = -n
		}
		fermentOffset = n
	})
	return fermentOffset
}
