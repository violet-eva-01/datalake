// Package tencent @author: Violet-Eva @date  : 2024/12/17 @notes :
package tencent

import (
	"github.com/fatih/color"
	"regexp"
	"strings"
	"sync"
	"time"
)

func CosMetaParse(input []CosInformation, lengths ...int) []CosInformationParse {

	var (
		length int
		output []CosInformationParse
	)

	if len(lengths) == 0 || (len(lengths) > 0 && lengths[0] == 0) {
		length = len(input)/5 + 1
	} else {
		length = lengths[0]
	}

	ciMap := CiSplit(length, input)

	var (
		wg sync.WaitGroup
		ch = make(chan []CosInformationParse, len(ciMap))
	)

	for _, ci := range ciMap {
		wg.Add(1)
		go cosMetaParseCI(&wg, ci, ch)
	}
	wg.Wait()

	close(ch)

	for result := range ch {
		output = append(output, result...)
	}

	return output
}

func cosMetaParseCI(wg *sync.WaitGroup, input []CosInformation, ch chan []CosInformationParse) {
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
			isTable := len(pathArr) > 5
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
