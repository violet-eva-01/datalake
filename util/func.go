// Package util @author: Violet-Eva @date  : 2024/11/25 @notes :
package util

import (
	"fmt"
	"github.com/fatih/color"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"sort"
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

func ListSplit(input []string, length int) map[int][]string {

	times := len(input) / length // 10001 / 2001 = 4
	output := make(map[int][]string, times+1)
	residual := len(input) % length // 10001 % 2001 = 1997

	if times == 0 || (times == 1 && residual == 0) {
		output[0] = input
	} else {
		if residual == 0 {
			times -= 1
		}

		starLen := 0
		endLen := length
		for index := 0; index <= times; index++ {
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

func PrintStruct(data ...any) {
	for index, v := range data {
		typeOf := reflect.TypeOf(v)
		valueOf := reflect.ValueOf(v)
		fmt.Printf("start print struct [%s] index [%d]", typeOf.Name(), index)
		for i := 0; i < typeOf.NumField(); i++ {
			fmt.Printf("type name: %+40v\ttype value: %-50v\n", typeOf.Field(i).Name, valueOf.Field(i).Interface())
		}
		fmt.Printf("print struct [%s] index [%d] end", typeOf.Name(), index)
	}
}

// ParseStructTags
// @Description:
// @param data
// @return []string
func ParseStructTags(data any, tagName string, splitKey ...string) []map[string]string {

	valueOf := reflect.ValueOf(data)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return nil
	}

	var output []map[string]string
	if len(splitKey) > 0 {
		for i := 0; i < valueOf.NumField(); i++ {
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tagValue := tag.Get(tagName)
			fieldType := field.Type.String()
			if tagValue != "" {
				splitValue := strings.Split(tagValue, ",")
				for _, Value := range splitValue {
					if strings.HasPrefix(Value, splitKey[0]) {
						tmpColumn := map[string]string{}
						columnName := strings.TrimPrefix(Value, splitKey[0])
						tmpColumn[columnName] = fieldType
						output = append(output, tmpColumn)
					}
				}
			}
		}
	} else {
		for i := 0; i < valueOf.NumField(); i++ {
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tagValue := tag.Get(tagName)
			fieldType := field.Type.String()
			tmpColumn := map[string]string{}
			tmpColumn[tagValue] = fieldType
			output = append(output, tmpColumn)
		}
	}

	return output
}

func ParseStructGormTags(data any) []map[string]string {
	return ParseStructTags(data, "gorm", "column:")
}

func ParseStructJsonTags(data any) []map[string]string {
	return ParseStructTags(data, "json")
}
