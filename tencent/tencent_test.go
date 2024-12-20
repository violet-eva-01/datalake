// Package tencent @author: Violet-Eva @date  : 2024/12/18 @notes :
package tencent

import (
	"fmt"
	"regexp"
	"testing"
)

func TestName(t *testing.T) {
	a := "ods_sox.db"
	compile := regexp.MustCompile("(?i)\\.db")
	matchString := compile.MatchString(a)
	fmt.Println(matchString)
	allString := compile.ReplaceAllString(a, "")
	fmt.Println(allString)
}
