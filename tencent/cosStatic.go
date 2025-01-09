// Package tencent @author: Violet-Eva @date  : 2024/12/17 @notes :
package tencent

import (
	"fmt"
	"github.com/fatih/color"
	"regexp"
	"strings"
	"sync"
	"time"
)

type CosInformation struct {
	Name     string `gorm:"column:name" json:"name"`
	Atime    string `gorm:"column:atime" json:"atime"`
	Mtime    string `gorm:"column:mtime" json:"mtime"`
	Ctime    string `gorm:"column:ctime" json:"ctime"`
	SizeByte int64  `gorm:"column:size_byte" json:"size_byte"`
	// DIR FILE
	Type     string `gorm:"column:type" json:"type"`
	Acl      string `gorm:"column:acl" json:"acl"`
	User     string `gorm:"column:user" json:"user"`
	Group    string `gorm:"column:group" json:"group"`
	FileType string `gorm:"column:file_type" json:"file_type"`
	DT       string `gorm:"column:dt" json:"dt"`
}

func CiSplit(length int, ciArr []CosInformation) map[int][]CosInformation {

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

type CIStreamlineParse struct {
	Name             string `gorm:"column:name" json:"name" spark:"name"`
	PathLevel        int    `gorm:"column:path_level" json:"path_level" spark:"path_level"`
	ExtendLevel0Name string `gorm:"column:extend_level0_name" json:"extend_level0_name" spark:"extend_level0_name"`
	ExtendLevel1Name string `gorm:"column:extend_level1_name" json:"extend_level1_name" spark:"extend_level1_name"`
	ExtendLevel2Name string `gorm:"column:extend_level2_name" json:"extend_level2_name" spark:"extend_level2_name"`
	ExtendLevel3Name string `gorm:"column:extend_level3_name" json:"extend_level3_name" spark:"extend_level3_name"`
	ExtendLevel4Name string `gorm:"column:extend_level4_name" json:"extend_level4_name" spark:"extend_level4_name"`
	ExtendLevel5Name string `gorm:"column:extend_level5_name" json:"extend_level5_name" spark:"extend_level5_name"`
	ExtendLevel6Name string `gorm:"column:extend_level6_name" json:"extend_level6_name" spark:"extend_level6_name"`
	TableName        string `gorm:"column:table_name" json:"table_name" spark:"table_name"`
	DBName           string `gorm:"column:db_name" json:"db_name" spark:"db_name"`
	TBLName          string `gorm:"column:tbl_name" json:"tbl_name" spark:"tbl_name"`
	Type             int    `gorm:"column:type" json:"type" spark:"type"`
	Size             int64  `gorm:"column:size" json:"size" spark:"size"`
}

func (c *CIStreamlineParse) parse(ci CosInformation) error {
	pathArr := strings.Split(ci.Name, "/")
	c.Name = ci.Name
	c.ExtendLevel0Name = "/"
	if len(pathArr) == 2 && pathArr[1] == "" {
		c.PathLevel = 0
	} else {
		c.PathLevel = len(pathArr) - 1
	}
	isDir := strings.Contains(strings.ToLower(ci.Type), "dir")
	continueSign := false
	switch {
	case len(pathArr) >= 7:
		if isDir {
			c.ExtendLevel6Name = pathArr[6]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 6:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel5Name = pathArr[5]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 5:
		compile := regexp.MustCompile("(?i)\\.db")
		isDatabase := compile.Match([]byte(pathArr[4]))
		isTable := len(pathArr) > 5
		if isDatabase && isTable {
			c.DBName = strings.ToLower(compile.ReplaceAllString(pathArr[4], ""))
			c.TBLName = strings.ToLower(pathArr[5])
			c.TableName = c.DBName + "." + c.TBLName
		}
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel4Name = pathArr[4]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 4:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel3Name = pathArr[3]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 3:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel2Name = pathArr[2]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 2:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel1Name = pathArr[1]
		} else {
			continueSign = true
		}
	default:
		return fmt.Errorf("abnormal data: %+v", ci)
	}
	switch ci.Type {
	case "FILE":
		c.Type = 2
	case "DIR":
		c.Type = 1
	default:
		return fmt.Errorf("abnormal data: %+v", ci)
	}
	c.Size = ci.SizeByte
	return nil
}

func CisPSplit(length int, ciArr []CIStreamlineParse) map[int][]CIStreamlineParse {

	times := len(ciArr) / length
	output := make(map[int][]CIStreamlineParse, times+1)
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

func cosMetaParseCIS(wg *sync.WaitGroup, input []CosInformation, ch chan []CIStreamlineParse) {
	var result []CIStreamlineParse
	defer wg.Done()
	defer func() {
		ch <- result
	}()
	for _, ci := range input {
		tmpCIP := CIStreamlineParse{}
		err := tmpCIP.parse(ci)
		if err != nil {
			color.Red(err.Error())
			continue
		}
		result = append(result, tmpCIP)
	}
}

func CosMetaCISParse(input []CosInformation, lengths ...int) []CIStreamlineParse {

	var (
		length int
		output []CIStreamlineParse
	)

	if len(lengths) == 0 || (len(lengths) > 0 && lengths[0] == 0) {
		length = len(input)/5 + 1
	} else {
		length = lengths[0]
	}

	ciMap := CiSplit(length, input)

	var (
		wg sync.WaitGroup
		ch = make(chan []CIStreamlineParse, len(ciMap))
	)

	for _, ci := range ciMap {
		wg.Add(1)
		go cosMetaParseCIS(&wg, ci, ch)
	}
	wg.Wait()

	close(ch)

	for result := range ch {
		output = append(output, result...)
	}

	return output
}

type CosInformationParse struct {
	Name             string    `gorm:"column:name" json:"name" spark:"name"`
	PathLevel        int       `gorm:"column:path_level" json:"path_level" spark:"path_level"`
	ExtendLevel0Name string    `gorm:"column:extend_level0_name" json:"extend_level0_name" spark:"extend_level0_name"`
	ExtendLevel1Name string    `gorm:"column:extend_level1_name" json:"extend_level1_name" spark:"extend_level1_name"`
	ExtendLevel2Name string    `gorm:"column:extend_level2_name" json:"extend_level2_name" spark:"extend_level2_name"`
	ExtendLevel3Name string    `gorm:"column:extend_level3_name" json:"extend_level3_name" spark:"extend_level3_name"`
	ExtendLevel4Name string    `gorm:"column:extend_level4_name" json:"extend_level4_name" spark:"extend_level4_name"`
	ExtendLevel5Name string    `gorm:"column:extend_level5_name" json:"extend_level5_name" spark:"extend_level5_name"`
	ExtendLevel6Name string    `gorm:"column:extend_level6_name" json:"extend_level6_name" spark:"extend_level6_name"`
	TableName        string    `gorm:"column:table_name" json:"table_name" spark:"table_name"`
	DBName           string    `gorm:"column:db_name" json:"db_name" spark:"db_name"`
	TBLName          string    `gorm:"column:tbl_name" json:"tbl_name" spark:"tbl_name"`
	Type             int       `gorm:"column:type" json:"type" spark:"type"`
	Size             int64     `gorm:"column:size" json:"size" spark:"size"`
	Atime            string    `gorm:"column:atime" json:"atime" spark:"atime"`
	Mtime            string    `gorm:"column:mtime" json:"mtime" spark:"mtime"`
	Ctime            string    `gorm:"column:ctime" json:"ctime" spark:"ctime"`
	Acl              string    `gorm:"column:acl"   json:"acl" spark:"acl"`
	User             string    `gorm:"column:user"  json:"user" spark:"user"`
	Group            string    `gorm:"column:group" json:"group" spark:"group"`
	IsTable          bool      `gorm:"column:is_table" json:"is_table" spark:"is_table"`
	DT               time.Time `gorm:"column:dt"    json:"dt" spark:"dt"`
}

func (c *CosInformationParse) parse(ci CosInformation, hiveMeta map[string]string) error {
	pathArr := strings.Split(ci.Name, "/")
	c.Name = ci.Name
	c.ExtendLevel0Name = "/"
	if len(pathArr) == 2 && pathArr[1] == "" {
		c.PathLevel = 0
	} else {
		c.PathLevel = len(pathArr) - 1
	}
	isDir := strings.Contains(strings.ToLower(ci.Type), "dir")
	continueSign := false
	switch {
	case len(pathArr) >= 7:
		if isDir {
			c.ExtendLevel6Name = pathArr[6]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 6:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel5Name = pathArr[5]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 5:
		compile := regexp.MustCompile("(?i)\\.db")
		isDatabase := compile.Match([]byte(pathArr[4]))
		isTable := len(pathArr) > 5
		if isDatabase && isTable {
			c.DBName = strings.ToLower(compile.ReplaceAllString(pathArr[4], ""))
			c.TBLName = strings.ToLower(pathArr[5])
			c.TableName = c.DBName + "." + c.TBLName
		}
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel4Name = pathArr[4]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 4:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel3Name = pathArr[3]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 3:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel2Name = pathArr[2]
		} else {
			continueSign = true
		}
		fallthrough
	case len(pathArr) == 2:
		if isDir || (!isDir && continueSign) {
			c.ExtendLevel1Name = pathArr[1]
		} else {
			continueSign = true
		}
	default:
		return fmt.Errorf("abnormal data: %+v", ci)
	}
	if len(hiveMeta) > 0 {
		for k, v := range hiveMeta {
			compile := regexp.MustCompile(fmt.Sprintf("(?i)%s", k))
			if compile.MatchString(ci.Name) {
				c.TableName = v
				c.DBName = strings.Split(v, ".")[0]
				c.TBLName = strings.Split(v, ".")[1]
				c.IsTable = true
			}
		}
	}
	switch ci.Type {
	case "FILE":
		c.Type = 2
	case "DIR":
		c.Type = 1
	default:
		return fmt.Errorf("abnormal data: %+v", ci)
	}
	c.Size = ci.SizeByte
	c.Atime = ci.Atime
	c.Mtime = ci.Mtime
	c.Ctime = ci.Ctime
	c.Acl = ci.Acl
	c.User = ci.User
	c.Group = ci.Group
	c.DT, _ = time.Parse("2006-01-02", ci.DT)
	return nil
}

func CipSplit(length int, ciArr []CosInformationParse) map[int][]CosInformationParse {

	times := len(ciArr) / length
	output := make(map[int][]CosInformationParse, times+1)
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

func cosMetaParseCI(wg *sync.WaitGroup, input []CosInformation, ch chan []CosInformationParse, hiveMeta map[string]string) {
	var result []CosInformationParse
	defer wg.Done()
	defer func() {
		ch <- result
	}()
	for _, ci := range input {
		tmpCIP := CosInformationParse{}
		err := tmpCIP.parse(ci, hiveMeta)
		if err != nil {
			color.Red(err.Error())
			continue
		}
		result = append(result, tmpCIP)
	}
}

func CosMetaCIParse(input []CosInformation, hiveMeta map[string]string, lengths ...int) []CosInformationParse {

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
		go cosMetaParseCI(&wg, ci, ch, hiveMeta)
	}
	wg.Wait()

	close(ch)

	for result := range ch {
		output = append(output, result...)
	}

	return output
}
