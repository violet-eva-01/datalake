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

func ciSplit(length int, ciArr []CosInformation) map[int][]CosInformation {

	times := len(ciArr) / length
	output := make(map[int][]CosInformation, times+1)
	residual := len(ciArr) % length

	if times == 0 || (times == 1 && residual == 0) {
		output[0] = ciArr
	} else {
		if residual == 0 {
			times -= 1
		}

		starLen := 0
		endLen := length
		for index := 0; index <= times; index++ {
			output[index] = ciArr[starLen:endLen]
			starLen += length
			if residual != 0 && index == times-1 {
				endLen += residual
			} else {
				endLen += length
			}
		}
	}

	return output
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
		ciMap = ciSplit(length, c.CI)
	}

	var (
		wg sync.WaitGroup
		ch = make(chan []CosInformationParse, len(ciMap))
	)
	for index, _ := range ciMap {
		information := ciMap[index]
		var tmpLength int
		tmpLength = len(information) / times
		tmpCiMap := ciSplit(tmpLength, information)
		for _, ci := range tmpCiMap {
			wg.Add(1)
			go parseCI(&wg, ci, ch)
		}
	}
	wg.Wait()
	close(ch)

	for result := range ch {
		c.CPI = append(c.CPI, result...)
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
		switch {
		case len(pathArr) >= 7:
			tmpCIP.ExtendLevel6Name = pathArr[6]
			fallthrough
		case len(pathArr) == 6:
			tmpCIP.ExtendLevel5Name = pathArr[5]
			fallthrough
		case len(pathArr) == 5:
			compile := regexp.MustCompile("(?i)\\.db")
			isDatabase := compile.Match([]byte(pathArr[4]))
			isTable := strings.Contains(strings.ToLower(ci.Type), "dir") && len(pathArr) > 5
			if isDatabase && isTable {
				tmpCIP.DBName = strings.ToLower(compile.ReplaceAllString(pathArr[4], ""))
				tmpCIP.TBLName = strings.ToLower(pathArr[5])
				tmpCIP.TableName = tmpCIP.DBName + "." + tmpCIP.TBLName
			}
			tmpCIP.ExtendLevel4Name = pathArr[4]
			fallthrough
		case len(pathArr) == 4:
			tmpCIP.ExtendLevel3Name = pathArr[3]
			fallthrough
		case len(pathArr) == 3:
			tmpCIP.ExtendLevel2Name = pathArr[2]
			fallthrough
		case len(pathArr) == 2:
			tmpCIP.ExtendLevel1Name = pathArr[1]
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
		result = append(result, tmpCIP)
	}
}
