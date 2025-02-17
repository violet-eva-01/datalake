// Package util @author: Violet-Eva @date  : 2024/11/25 @notes :
package util

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/xuri/excelize/v2"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

func RandomPassword(limits ...[4]int) (str string, err error) {

	var limit [4]int
	if len(limits) <= 0 {
		limit = [4]int{1, 1, 1, 1}
	} else {
		limit = limits[0]
	}
	rand.NewSource(time.Now().UnixNano())
	digits := []byte("0123456789")
	lowers := []byte("abcdefghijklmnopqrstuvwxyz")
	uppers := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	chars := []byte(",.<>!@#$%^&*()_=-[]{}|;:/?")
	byteS := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	passwordLength := 18 + rand.Intn(6)
	color.Green("密码长度为: %d", passwordLength)
	leftPasswordLength := passwordLength - limit[0] - limit[1] - limit[2] - limit[3]
	if leftPasswordLength < 0 {
		err = fmt.Errorf("密码限制为:[%d]位数字,[%d]位小写字母,[%d]位大写字母.已超过密码的长度[%d],请重新指定密码限制", limit[0], limit[1], limit[2], passwordLength)
		return
	}
	var result []byte
	color.Green("至少取[%d]位数字", limit[0])
	for i := 0; i < limit[0]; i++ {
		result = append(result, byteS[rand.Intn(len(digits))])
	}
	color.Green("至少取[%d]位小写字母", limit[1])
	for i := 0; i < limit[1]; i++ {
		result = append(result, byteS[rand.Intn(len(lowers))])
	}
	color.Green("至少取[%d]位大写字母", limit[2])
	for i := 0; i < limit[2]; i++ {
		result = append(result, byteS[rand.Intn(len(uppers))])
	}
	color.Green("至少取[%d]位特殊字符", limit[3])
	for i := 0; i < limit[2]; i++ {
		result = append(result, byteS[rand.Intn(len(chars))])
	}
	rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < leftPasswordLength; i++ {
		result = append(result, byteS[rand.Intn(len(byteS))])
	}
	rand.Shuffle(len(result), func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	str = string(result)
	return
}

func StringSliceIntersection(slice1, slice2 []string) []string {

	elements := make(map[string]bool)
	intersect := make([]string, 0)

	for _, v := range slice1 {
		elements[v] = true
	}

	for _, v := range slice2 {
		if elements[v] {
			intersect = append(intersect, v)
			delete(elements, v)
		}
	}

	sort.Strings(intersect)

	return intersect
}

func In(str string, strArray []string, isSort bool) bool {
	if isSort {
		sort.Strings(strArray)
	}
	index := sort.SearchStrings(strArray, str)
	if index < len(strArray) && strArray[index] == str {
		return true
	}
	return false
}

func FindIndex(str string, strArr []string) int {
	for index, element := range strArr {
		if str == element {
			return index
		}
	}
	return -1
}

func RemoveCoincideElement(list1, list2 []string, isSort bool) []string {
	result := make([]string, 0)
	for _, i := range list1 {
		if !In(i, list2, isSort) {
			result = append(result, i)
		}
	}
	return result
}

func Match(str string, strArray []string) bool {
	for _, i := range strArray {
		if strings.Contains(str, i) {
			return true
		}
	}
	return false
}

func RemoveMatchElement(list1, list2 []string) []string {
	result := make([]string, 0)
	for _, i := range list1 {
		if !Match(i, list2) {
			result = append(result, i)
		}
	}
	return result
}

func RemoveRepeatElement(list []string) []string {
	temp := make(map[string]struct{})
	index := 0
	for _, v := range list {
		v = strings.TrimSpace(v)
		temp[v] = struct{}{}
	}
	tempList := make([]string, len(temp))
	for key := range temp {
		tempList[index] = key
		index++
	}
	return tempList
}

func RemoveRepeatElementAndToLower(list []string) []string {
	temp := make(map[string]struct{})
	index := 0
	for _, v := range list {
		v = strings.ToLower(strings.TrimSpace(v))
		temp[v] = struct{}{}
	}
	tempList := make([]string, len(temp))
	for key := range temp {
		tempList[index] = key
		index++
	}
	return tempList
}

func SListSplit(input []string, length int) [][]string {

	times := len(input) / length    // 10001 / 2001 = 4
	residual := len(input) % length // 10001 % 2001 = 1997
	if residual > 0 {
		times += 1
	}
	output := make([][]string, times)

	if times <= 1 {
		output[0] = input
	} else {
		starLen := 0
		endLen := length
		for index := 0; index < times; index++ {
			output[index] = input[starLen:endLen]
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

func MapSListSplit(input []map[string]string, length int) [][]map[string]string {
	times := len(input) / length    // 10001 / 2001 = 4
	residual := len(input) % length // 10001 % 2001 = 1997
	if residual > 0 {
		times += 1
	}
	output := make([][]map[string]string, times)

	if times <= 1 {
		output[0] = input
	} else {
		starLen := 0
		endLen := length
		for index := 0; index < times; index++ {
			output[index] = input[starLen:endLen]
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

func MapIListSplit(input []map[string]interface{}, length int) [][]map[string]interface{} {
	times := len(input) / length    // 10001 / 2001 = 4
	residual := len(input) % length // 10001 % 2001 = 1997
	if residual > 0 {
		times += 1
	}
	output := make([][]map[string]interface{}, times)

	if times <= 1 {
		output[0] = input
	} else {
		starLen := 0
		endLen := length
		for index := 0; index < times; index++ {
			output[index] = input[starLen:endLen]
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

func SetRequestBasicAuth(request *http.Request, username string, password string) {
	request.SetBasicAuth(username, password)
}

func SetRequestHeader(request *http.Request, mssArr ...map[string]string) {
	for _, mss := range mssArr {
		for key, value := range mss {
			request.Header.Set(key, value)
		}
	}
}

func GetResponse(request *http.Request, proxy string) (resp *http.Response, err error) {

	var (
		proxyUrl *url.URL
	)

	if proxy != "" {
		proxyUrl, err = url.Parse(proxy)
		if err != nil {
			return
		}
		resp, err = (&http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxyUrl),
			},
		}).Do(request)
	} else {
		resp, err = (&http.Client{}).Do(request)
	}

	return
}

func PrintStruct(data any) {
	typeOf := reflect.TypeOf(data)
	valueOf := reflect.ValueOf(data)
	color.Blue("start print struct [%s]\n", typeOf.Name())
	for i := 0; i < typeOf.NumField(); i++ {
		color.Green("type name: %+40v\ttype value: %-50v\n", typeOf.Field(i).Name, valueOf.Field(i).Interface())
	}
	color.Blue("print struct [%s] end\n", typeOf.Name())

}

// ConvStructTags
// @Description: get tag name & tag elem type or get elem name & tag name
// @param data
// @param tagName
// @param isGetType  true , get tag name & tag elem type . false , get elem name & tag name.
// @param splitKey
// @return map[string]string
func ConvStructTags(data any, tagName string, isGetType bool, splitKey ...string) map[string]string {

	valueOf := reflect.ValueOf(data)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return nil
	}

	output := make(map[string]string, valueOf.NumField())
	if len(splitKey) > 0 {
		for i := 0; i < valueOf.NumField(); i++ {
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tagValue := tag.Get(tagName)
			var fieldType string
			if isGetType {
				fieldType = field.Type.String()
			} else {
				fieldType = field.Name
			}
			if tagValue != "" {
				splitValue := strings.Split(tagValue, ",")
				for _, Value := range splitValue {
					if strings.HasPrefix(Value, splitKey[0]) {
						columnName := strings.TrimPrefix(Value, splitKey[0])
						if isGetType {
							output[columnName] = fieldType
						} else {
							output[fieldType] = columnName
						}
					}
				}
			}
		}
	} else {
		for i := 0; i < valueOf.NumField(); i++ {
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tagValue := tag.Get(tagName)
			var fieldType string
			if isGetType {
				fieldType = field.Type.String()
			} else {
				fieldType = field.Name
			}
			if isGetType {
				// tag name : elem type
				output[tagValue] = fieldType
			} else {
				// elem name : tag name
				output[fieldType] = tagValue
			}
		}
	}

	return output
}

func ConvStructGormTags(data any, isGetType bool) map[string]string {
	return ConvStructTags(data, "gorm", isGetType, "column:")
}

func ConvStructJsonTags(data any, isGetType bool) map[string]string {
	return ConvStructTags(data, "json", isGetType)
}

func ConvStructSparkTags(data any, isGetType bool) map[string]string {
	return ConvStructTags(data, "spark", isGetType)
}

func ConvStructDoubleTags(data any, tagName1, tagName2 string, splitKey ...[2]string) map[string]string {
	valueOf := reflect.ValueOf(data)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return nil
	}
	output := make(map[string]string, valueOf.NumField())
	if len(splitKey) > 0 {
		for i := 0; i < valueOf.NumField(); i++ {
			var (
				tag1Name string
				tag2Name string
			)
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tag1Value := tag.Get(tagName1)
			if tag1Value != "" {
				splitValue := strings.Split(tag1Value, ",")
				for _, Value := range splitValue {
					if strings.HasPrefix(Value, splitKey[0][0]) {
						tag1Name = strings.TrimPrefix(Value, splitKey[0][0])
					}
				}
			}
			tag2Value := tag.Get(tagName2)
			if tag2Value != "" {
				splitValue := strings.Split(tag1Value, ",")
				for _, Value := range splitValue {
					if strings.HasPrefix(Value, splitKey[0][0]) {
						tag2Name = strings.TrimPrefix(Value, splitKey[0][0])
					}
				}
			}
			output[tag1Name] = tag2Name
		}
	} else {
		for i := 0; i < valueOf.NumField(); i++ {
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tag1Value := tag.Get(tagName1)
			tag2Value := tag.Get(tagName2)
			output[tag1Value] = tag2Value
		}
	}
	return output
}

func MapTurnOver(input map[string]string) map[string]string {
	var output = make(map[string]string, len(input))
	for key, value := range input {
		output[value] = key
	}
	return output
}

func findLongKeyAndLongValue(data []map[string]string) (maxKeyLen, maxValueLen int) {

	for _, value := range data {
		for k, v := range value {
			if len(k) > maxKeyLen {
				maxKeyLen = len(k)
			}
			if len(v) > maxValueLen {
				maxValueLen = len(v)
			}
		}
	}

	return
}

func FormatKeyValueToSQL(sqlType string, data []map[string]string, input ...[2]string) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("data is empty")
	}
	var (
		createSQL []string
		dbName    string
		tblName   string
		startSQL  string
		endSQL    string
		fmtT      string
		typeIndex int
	)
	if len(input) > 0 {
		dbName = input[0][0]
		tblName = input[0][1]
	} else {
		dbName = "default_db"
		tblName = "default_tbl"
	}

	switch sqlType {
	case "create":
		typeIndex = 1
		maxKeyLen, maxValueLen := findLongKeyAndLongValue(data)
		fmtT = fmt.Sprint("    %-", maxKeyLen+10, "s    %-", maxValueLen+10, "s")
		startSQL = fmt.Sprintf("create table %s.%s (", dbName, tblName)
		endSQL = ")"
	case "select":
		typeIndex = 2
		fmtT = "%s"
		startSQL = "select"
		endSQL = fmt.Sprintf("from %s.%s", dbName, tblName)
	default:
		return "", fmt.Errorf("sql type not support")
	}

	for index, value := range data {
		for k, v := range value {
			k = "`" + k + "`"
			var tmpCreateSQL string
			switch index {
			case len(data) - 1:
				switch typeIndex {
				case 1:
					tmpCreateSQL = fmt.Sprintf(fmtT, k, v)
				case 2:
					tmpCreateSQL = fmt.Sprintf(fmtT, k)
				}
				createSQL = append(createSQL, tmpCreateSQL)
				tmpCreateSQL = endSQL
			case 0:
				tmpCreateSQL = startSQL
				createSQL = append(createSQL, tmpCreateSQL)
				fallthrough
			default:
				switch typeIndex {
				case 1:
					tmpCreateSQL = fmt.Sprintf(fmtT+",", k, v)
				case 2:
					tmpCreateSQL = fmt.Sprintf(fmtT+",", k)
				}
			}
			createSQL = append(createSQL, tmpCreateSQL)
		}
	}
	return strings.Join(createSQL, "\n"), nil
}

func WriteExcelForSlice(fileName, path string, sheetName, title []string, data ...[]interface{}) (err error) {

	var (
		titles     []string
		sheetNames []string
		dataType   bool
	)

	if len(data) < 1 {
		return errors.Errorf("没有数据传入,无法写Excel文档")
	}

	if sheetName == nil || len(sheetName) < 1 {
		for i := 1; i <= len(data); i++ {
			sheetNames = append(sheetNames, fmt.Sprintf("第%d页", i))
		}
	} else {
		sheetNames = sheetName
	}

	tmpData := data[0][0]
	tmpRowType := reflect.TypeOf(tmpData)
	tmpRowTypeStr := tmpRowType.Kind().String()

	if tmpRowTypeStr == "struct" {
		dataType = true
	}

	if title == nil || (len(title) < 1 && dataType) {
		for index := 0; index < tmpRowType.NumField(); index++ {
			titles = append(titles, tmpRowType.Field(index).Name)
		}
	} else {
		titles = title
	}

	excelFile := excelize.NewFile()

	switch {
	case dataType:

		for sheetIndex, sheetData := range data {
			sn := sheetNames[sheetIndex]
			_, err = excelFile.NewSheet(sn)
			if err != nil {
				return
			}

			for titleIndex, colTitle := range titles {
				cell, _ := excelize.CoordinatesToCellName(titleIndex+1, 1)
				err = excelFile.SetCellValue(sn, cell, colTitle)
				if err != nil {
					return
				}
			}

			for rowIndex, row := range sheetData {
				rowType := reflect.TypeOf(row)
				rowValue := reflect.ValueOf(row)
				for columnIndex := 0; columnIndex < rowType.NumField(); columnIndex++ {
					switch rowValue.Field(columnIndex).String() {
					case "int", "int8", "int16", "int32", "int64":
						cell, _ := excelize.CoordinatesToCellName(columnIndex+1, rowIndex+2)
						content := rowValue.Field(columnIndex).Int()
						_ = excelFile.SetCellInt(sn, cell, int(content))
						style, _ := excelFile.NewStyle(&excelize.Style{NumFmt: 1})
						_ = excelFile.SetCellStyle(sn, cell, cell, style)
					case "float32", "float64":
						cell, _ := excelize.CoordinatesToCellName(columnIndex+1, rowIndex+2)
						content := rowValue.Field(columnIndex).Float()
						_ = excelFile.SetCellFloat(sn, cell, content, -1, 64)
						style, _ := excelFile.NewStyle(&excelize.Style{NumFmt: 2})
						_ = excelFile.SetCellStyle(sn, cell, cell, style)
					default:
						content := rowValue.Field(columnIndex).Interface()
						cell, _ := excelize.CoordinatesToCellName(columnIndex+1, rowIndex+2)
						_ = excelFile.SetCellValue(sn, cell, content)
					}
				}
			}
		}

		_ = excelFile.DeleteSheet("Sheet1")

	case !dataType:

		for sheetIndex, sheetData := range data {
			sn := "Sheet1"
			if sheetIndex != 0 {
				sn = fmt.Sprintf("Sheet%d", sheetIndex+1)
				_, err = excelFile.NewSheet(sn)
				if err != nil {
					return
				}
			}
			cell, _ := excelize.CoordinatesToCellName(1, 1)
			err = excelFile.SetCellValue(sn, cell, tmpRowType.Kind().String())
			if err != nil {
				return
			}
			for rowIndex, row := range sheetData {
				rowStr := fmt.Sprintf("%v", row)
				switch tmpRowTypeStr {
				case "int", "int8", "int16", "int32", "int64":
					rowCell, _ := excelize.CoordinatesToCellName(1, rowIndex+2)
					content, _ := strconv.Atoi(rowStr)
					_ = excelFile.SetCellInt(sn, rowCell, int(content))
					style, _ := excelFile.NewStyle(&excelize.Style{NumFmt: 1})
					_ = excelFile.SetCellStyle(sn, rowCell, rowCell, style)
				case "float32", "float64":
					rowCell, _ := excelize.CoordinatesToCellName(1, rowIndex+2)
					content, _ := strconv.ParseFloat(rowStr, 64)
					_ = excelFile.SetCellFloat(sn, rowCell, content, -1, 64)
					style, _ := excelFile.NewStyle(&excelize.Style{NumFmt: 2})
					_ = excelFile.SetCellStyle(sn, rowCell, rowCell, style)
				default:
					rowCell, _ := excelize.CoordinatesToCellName(1, rowIndex+2)
					_ = excelFile.SetCellValue(sn, rowCell, rowStr)
				}
			}
		}
	}

	excelFileName := fileName + ".xlsx"

	err = excelFile.SaveAs(path + "/" + excelFileName)
	if err != nil {
		return
	}
	return
}

type judgeS struct {
	Index  int
	Length int
}

func GetMaxLenMapList(input []map[string]interface{}) map[string]interface{} {

	var js judgeS
	for index, value := range input {
		length := len(value)
		if index == 0 {
			js.Index = index
			js.Length = length
		} else {
			if length > js.Length {
				js.Index = index
				js.Length = length
			}
		}
	}
	return input[js.Index]
}

func WriteExcelForMapList(fileName, path string, sheetName, title []string, data ...[]map[string]interface{}) (err error) {

	var (
		titles     []string
		sheetNames []string
	)

	if len(data) < 1 {
		return errors.Errorf("没有数据传入,无法写Excel文档")
	}

	if sheetName == nil || len(sheetName) < 1 {
		for i := 1; i <= len(data); i++ {
			sheetNames = append(sheetNames, fmt.Sprintf("第%d页", i))
		}
	} else {
		sheetNames = sheetName
	}

	excelFile := excelize.NewFile()

	for sheetIndex, sheetData := range data {
		sn := "Sheet1"
		if sheetIndex != 0 {
			sn = fmt.Sprintf("Sheet%d", sheetIndex+1)
			_, err = excelFile.NewSheet(sn)
			if err != nil {
				return
			}
		}
		if title == nil || (len(title) < 1) {
			list := GetMaxLenMapList(sheetData)
			for k := range list {
				titles = append(titles, k)
			}
		} else {
			titles = title
		}
		for titleIndex, colTitle := range titles {
			cell, _ := excelize.CoordinatesToCellName(titleIndex+1, 1)
			err = excelFile.SetCellValue(sn, cell, colTitle)
			if err != nil {
				return
			}
		}
		for rowIndex, row := range sheetData {
			for columnIndex, columnName := range titles {
				rowType := reflect.TypeOf(row[columnName]).Kind().String()
				switch rowType {
				case "int", "int8", "int16", "int32", "int64":
					cell, _ := excelize.CoordinatesToCellName(columnIndex+1, rowIndex+2)
					content := row[columnName].(int)
					_ = excelFile.SetCellInt(sn, cell, content)
					style, _ := excelFile.NewStyle(&excelize.Style{NumFmt: 1})
					_ = excelFile.SetCellStyle(sn, cell, cell, style)
				case "float32", "float64":
					cell, _ := excelize.CoordinatesToCellName(columnIndex+1, rowIndex+2)
					content := row[columnName].(float64)
					precision := len(strings.Split(fmt.Sprintf("%s", row[columnName]), ".")[1])
					_ = excelFile.SetCellFloat(sn, cell, content, precision, 64)
					style, _ := excelFile.NewStyle(&excelize.Style{NumFmt: 2})
					_ = excelFile.SetCellStyle(sn, cell, cell, style)
				default:
					content := row[columnName]
					cell, _ := excelize.CoordinatesToCellName(columnIndex+1, rowIndex+2)
					_ = excelFile.SetCellValue(sn, cell, content)
				}
			}
		}
	}

	excelFileName := fileName + ".xlsx"

	err = excelFile.SaveAs(path + "/" + excelFileName)
	if err != nil {
		return
	}
	return
}

func WriteToTextFile(fileName string, path string, data []byte) error {

	create, err := os.Create(path + "/" + fileName)
	if err != nil {
		return err
	}
	defer create.Close()
	err = os.WriteFile(fileName, data, 0777)
	if err != nil {
		return err
	}

	return nil
}
