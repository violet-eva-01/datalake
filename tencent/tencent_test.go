// Package tencent @author: Violet-Eva @date  : 2024/12/18 @notes :
package tencent

import (
	"fmt"
	"reflect"
	"testing"
)

type a struct {
	A int
	B int
}

func TestName(t *testing.T) {
	ac := a{1, 2}
	of := reflect.TypeOf(ac)
	fmt.Println(of.Name())
}
