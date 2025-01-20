// Package util @author: Violet-Eva @date  : 2025/1/20 @notes :
package util

import "testing"

func TestWriteExcel(t *testing.T) {
	aa := []map[string]interface{}{}
	a := make(map[string]interface{})
	a["a"] = "a"
	a["b"] = 1
	a["c"] = "c"
	a["d"] = 3
	a["e"] = 1.122
	b := make(map[string]interface{})
	b["a"] = "a"
	b["b"] = 1
	b["c"] = "c"
	b["d"] = 3
	b["e"] = 1.12211
	aa = append(aa, b)
	var i []interface{}
	for _, v := range aa {
		i = append(i, v)
	}
	tmp := WriteExcelForMapList("test", "./", []string{}, []string{}, aa)
	t.Log(tmp)
}
