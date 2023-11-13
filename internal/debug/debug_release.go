//go:build !debug
// +build !debug

package debug

import "fmt"

// Log prints a message to the debug log (if debug is enabled).
func Log(fmtin string, args ...interface{}) {
	fmt.Printf("[DEBUG] "+fmtin+"\n", args...)
}
