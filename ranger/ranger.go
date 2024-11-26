// Package ranger @author: Violet-Eva @date  : 2024/11/25 @notes :
package ranger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/violet-eva-01/datalake/util"
	"io"
	"net/http"
)

type Ranger struct {
	Host           string            `json:"host"`
	Port           int               `json:"port"`
	ApiPath        string            `json:"apiPath"`
	Proxy          string            `json:"proxy"`
	UserName       string            `json:"userName"`
	PassWord       string            `json:"password"`
	Headers        map[string]string `json:"headers"`
	ServiceTypeIds []ServiceTypeId   `json:"serviceTypeIds"`
	ServiceDefs    []ServiceDef      `json:"serviceDefs"`
	PolicyBodies   []PolicyBody      `json:"policyBodies"`
}

func NewRangerAll(host string, port int, apiPath string, proxy string, userName string, passWord string, tmpHeaders map[string]string) *Ranger {
	if apiPath == "" {
		apiPath = "service"
	}

	headers := make(map[string]string, len(tmpHeaders)+1)
	for key, value := range tmpHeaders {
		headers[key] = value
	}
	headers["Accept"] = "application/json"

	return &Ranger{
		Host:     host,
		Port:     port,
		ApiPath:  apiPath,
		Proxy:    proxy,
		UserName: userName,
		PassWord: passWord,
		Headers:  headers,
	}
}

func NewRanger(host string, userName string, passWord string, tmpProxy ...string) *Ranger {
	var (
		proxy   string
		headers = make(map[string]string)
	)

	headers["Accept"] = "application/json"

	if len(tmpProxy) > 0 {
		proxy = tmpProxy[0]
	}

	return &Ranger{
		Host:     host,
		Port:     6080,
		ApiPath:  "service",
		Proxy:    proxy,
		UserName: userName,
		PassWord: passWord,
		Headers:  headers,
	}
}

func (r *Ranger) Request(method string, Api string, body []byte) (*http.Response, error) {

	request, reqErr := http.NewRequest(method, fmt.Sprintf("http://%s:%d/%s%s", r.Host, r.Port, r.ApiPath, Api), bytes.NewBuffer(body))
	if reqErr != nil {
		return nil, reqErr
	}

	util.SetRequestBasicAuth(request, r.UserName, r.PassWord)
	util.SetRequestHeader(request, r.Headers)
	resp, respErr := util.GetResponse(request, r.Proxy)
	if respErr != nil {
		return nil, respErr
	}

	return resp, respErr
}

// RequestToStruct
// @Description:
// @param method 请求方法
// @param Api ranger api
// @param body 请求体
// @param data 需要为struct指针
// @return error
func (r *Ranger) RequestToStruct(method string, Api string, body []byte, data any) error {

	request, reqErr := http.NewRequest(method, fmt.Sprintf("http://%s:%d/%s%s", r.Host, r.Port, r.ApiPath, Api), bytes.NewBuffer(body))
	if reqErr != nil {
		return reqErr
	}

	util.SetRequestBasicAuth(request, r.UserName, r.PassWord)
	util.SetRequestHeader(request, r.Headers)
	resp, respErr := util.GetResponse(request, r.Proxy)
	if respErr != nil {
		return respErr
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}

	if respBody == nil {
		return errors.New("response body is nil")
	}

	juErr := json.Unmarshal(respBody, &data)
	if juErr != nil {
		return juErr
	}

	return respErr
}

func (r *Ranger) GetServiceDefs() error {
	pd := &PluginsDefinitions{}

	respErr := r.RequestToStruct("GET", "/plugins/definitions", nil, pd)
	if respErr != nil {
		return respErr
	}

	r.ServiceDefs = pd.ServiceDefs

	for _, sd := range r.ServiceDefs {
		for index, st := range serviceTypeName {
			if sd.Name == st {
				var tmpSTI ServiceTypeId
				tmpSTI.ServiceTypeId = index
				tmpSTI.ServiceType = ServiceType(index)
				r.ServiceTypeIds = append(r.ServiceTypeIds, tmpSTI)
				break
			}
		}
	}

	return nil
}

func (r *Ranger) GetServiceId(serviceType ServiceType) int {
	for _, sti := range r.ServiceTypeIds {
		if sti.ServiceType == serviceType {
			return sti.ServiceTypeId
		}
	}
	return -1
}

func (r *Ranger) GetPolicy(serviceTypes ...int) error {

	if len(serviceTypes) == 0 {
		pb := &[]PolicyBody{}
		respErr := r.RequestToStruct("GET", "/public/v2/api/policy?serviceType=hive", nil, pb)
		if respErr != nil {
			return respErr
		}
		r.PolicyBodies = append(r.PolicyBodies, *pb...)
	} else {
		for _, serviceType := range serviceTypes {
			if inServiceType(serviceType) {
				pb := &[]PolicyBody{}
				respErr := r.RequestToStruct("GET", "/public/v2/api/policy", nil, pb)
				if respErr != nil {
					return respErr
				}
				r.PolicyBodies = append(r.PolicyBodies, *pb...)
			}
		}
	}

	return nil
}
