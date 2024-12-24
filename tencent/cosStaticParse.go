// Package tencent @author: Violet-Eva @date  : 2024/12/17 @notes :
package tencent

import (
	"github.com/fatih/color"
	"regexp"
	"strings"
	"sync"
	"time"
)

type CosParse struct {
	WarehousePath string
	CI            []CosInformation
	CPI           []CosInformationParse
}

func NewCosParse(cis ...CosInformation) *CosParse {

	warehousePath := "/user/hive/warehouse"
	return &CosParse{
		WarehousePath: warehousePath,
		CI:            cis,
	}

}

func (c *CosParse) Parse(times int, lengths ...int) {

	var (
		length int
	)
	ciMap := map[int][]CosInformation{}
	if len(lengths) == 0 || (len(lengths) > 0 && lengths[0] == 0) {
		ciMap[0] = c.CI
	} else {
		length = lengths[0]
		ciMap = CiSplit(length, c.CI)
	}

	for i := 0; i < len(ciMap); i++ {
		var tmpLength int
		tmpLength = len(ciMap[i]) / times
		tmpCiMap := CiSplit(tmpLength, ciMap[i])
		var (
			wg sync.WaitGroup
			ch = make(chan []CosInformationParse, len(tmpCiMap))
		)
		for _, ci := range tmpCiMap {
			wg.Add(1)
			go parseCI(&wg, ci, ch)
		}
		wg.Wait()
		close(ch)

		for result := range ch {
			c.CPI = append(c.CPI, result...)
		}
	}
}

func parseCI(wg *sync.WaitGroup, input []CosInformation, ch chan []CosInformationParse) {
	var result []CosInformationParse
	defer wg.Done()
	defer func() {
		ch <- result
	}()
	for _, ci := range input {
		var tmpCIP CosInformationParse
		pathArr := strings.Split(ci.Name, "/")
		tmpCIP.Name = ci.Name
		tmpCIP.ExtendLevel0Name = "/"
		if len(pathArr) == 2 && pathArr[1] == "" {
			tmpCIP.PathLevel = 0
		} else {
			tmpCIP.PathLevel = len(pathArr) - 1
		}
		isDir := strings.Contains(strings.ToLower(ci.Type), "dir")
		continueSign := false
		switch {
		case len(pathArr) >= 7:
			if isDir {
				tmpCIP.ExtendLevel6Name = pathArr[6]
			} else {
				continueSign = true
			}
			fallthrough
		case len(pathArr) == 6:
			if isDir || (!isDir && continueSign) {
				tmpCIP.ExtendLevel5Name = pathArr[5]
			} else {
				continueSign = true
			}
			fallthrough
		case len(pathArr) == 5:
			compile := regexp.MustCompile("(?i)\\.db")
			isDatabase := compile.Match([]byte(pathArr[4]))
			isTable := isDir && len(pathArr) > 5
			if isDatabase && isTable {
				tmpCIP.DBName = strings.ToLower(compile.ReplaceAllString(pathArr[4], ""))
				tmpCIP.TBLName = strings.ToLower(pathArr[5])
				tmpCIP.TableName = tmpCIP.DBName + "." + tmpCIP.TBLName
			}
			if isDir || (!isDir && continueSign) {
				tmpCIP.ExtendLevel4Name = pathArr[4]
			} else {
				continueSign = true
			}
			fallthrough
		case len(pathArr) == 4:
			if isDir || (!isDir && continueSign) {
				tmpCIP.ExtendLevel3Name = pathArr[3]
			} else {
				continueSign = true
			}
			fallthrough
		case len(pathArr) == 3:
			if isDir || (!isDir && continueSign) {
				tmpCIP.ExtendLevel2Name = pathArr[2]
			} else {
				continueSign = true
			}
			fallthrough
		case len(pathArr) == 2:
			if isDir || (!isDir && continueSign) {
				tmpCIP.ExtendLevel1Name = pathArr[1]
			} else {
				continueSign = true
			}
		default:
			color.Red("abnormal data: %+v", ci)
			continue
		}
		tmpCIP.Type = ci.Type
		tmpCIP.Atime, _ = time.Parse("2006-01-02T15:04:05-0700", ci.Atime)
		tmpCIP.Mtime, _ = time.Parse("2006-01-02T15:04:05-0700", ci.Mtime)
		tmpCIP.Ctime, _ = time.Parse("2006-01-02T15:04:05-0700", ci.Ctime)
		tmpCIP.Size = ci.SizeByte
		tmpCIP.Acl = ci.Acl
		tmpCIP.User = ci.User
		tmpCIP.Group = ci.Group
		tmpCIP.DT = ci.DT
		result = append(result, tmpCIP)
	}
}
