// Package ranger @author: Violet-Eva @date  : 2024/11/25 @notes :
package ranger

import (
	"fmt"
	"testing"
)

func TestRanger(t *testing.T) {
	var s = make(map[string]string, 4)
	s["name"] = "aaaaa"
	s["type"] = "man"
	fmt.Println(s)
}
