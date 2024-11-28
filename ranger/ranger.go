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
	"strings"
)

type Ranger struct {
	Host                string                  `json:"host"`
	Port                int                     `json:"port"`
	ApiPath             string                  `json:"apiPath"`
	Proxy               string                  `json:"proxy"`
	UserName            string                  `json:"userName"`
	PassWord            string                  `json:"password"`
	Headers             map[string]string       `json:"headers"`
	ServiceTypeIds      []ServiceTypeId         `json:"serviceTypeIds"`
	ServiceDefs         []ServiceDef            `json:"serviceDefs"`
	ServicePolicyBodies map[string][]PolicyBody `json:"service_policy_bodies"`
	VXUsers             []VXUser                `json:"users"`
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
	headers["Content-Type"] = "application/json"

	return &Ranger{
		Host:                host,
		Port:                port,
		ApiPath:             apiPath,
		Proxy:               proxy,
		UserName:            userName,
		PassWord:            passWord,
		Headers:             headers,
		ServicePolicyBodies: make(map[string][]PolicyBody, len(serviceTypeName)),
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
		Host:                host,
		Port:                6080,
		ApiPath:             "service",
		Proxy:               proxy,
		UserName:            userName,
		PassWord:            passWord,
		Headers:             headers,
		ServicePolicyBodies: make(map[string][]PolicyBody, len(serviceTypeName)),
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
// @param data 需要为[struct | struct slice]指针
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

	respErr := r.RequestToStruct("GET", "/plugins/definitions?pageSize=99999", nil, pd)
	if respErr != nil {
		return respErr
	}

	r.ServiceDefs = pd.ServiceDefs

	for _, sd := range r.ServiceDefs {
		index := util.FindIndex(strings.ToLower(sd.Name), serviceTypeName)
		if index >= 0 {
			var tmpSTI ServiceTypeId
			tmpSTI.ServiceTypeId = index
			tmpSTI.ServiceType = ServiceType(index)
			r.ServiceTypeIds = append(r.ServiceTypeIds, tmpSTI)
		}
	}

	return nil
}

func (r *Ranger) GetPolicy(serviceTypeNames ...string) error {
	if len(serviceTypeNames) == 0 {
		for _, sti := range r.ServiceTypeIds {
			pb := &[]PolicyBody{}
			respErr := r.RequestToStruct("GET", fmt.Sprintf("/public/v2/api/policy?pageSize=999999&serviceType=%s", sti.ServiceType.String()), nil, pb)
			if respErr != nil {
				return respErr
			}
			r.ServicePolicyBodies[sti.ServiceType.String()] = *pb
		}
	} else {
		for _, serviceType := range serviceTypeNames {
			if index := util.FindIndex(strings.ToLower(serviceType), serviceTypeName); index >= 0 {
				pb := &[]PolicyBody{}
				respErr := r.RequestToStruct("GET", fmt.Sprintf("/public/v2/api/policy?pageSize=999999&serviceType=%s", serviceTypeNames[index]), nil, pb)
				if respErr != nil {
					return respErr
				}
				r.ServicePolicyBodies[serviceType] = *pb
			}
		}
	}
	return nil
}

func GetXUsersId(userName string) int {
	return tencentUserInformationIndex[userName]
}

func (r *Ranger) GetXUsers() error {
	xUsers := &XUsers{}
	err := r.RequestToStruct("GET", "/xusers/users", nil, xUsers)
	if err != nil {
		return err
	}

	r.VXUsers = xUsers.VXUsers

	for _, i := range r.VXUsers {
		tencentUserInformationIndex[i.Name] = i.Id
	}

	return nil
}

func (r *Ranger) ChangePassword(userId int, newPassword string) (userInformation *VXUser, err error) {

	var (
		reqBody []byte
	)

	err = r.RequestToStruct("GET", fmt.Sprintf("/xusers/secure/users/%d", userId), nil, userInformation)
	if err != nil {
		return
	}

	userInformation.Password = newPassword

	reqBody, err = json.Marshal(userInformation)
	if err != nil {
		return
	}

	err = r.RequestToStruct("PUT", fmt.Sprintf("/xusers/secure/users/%d", userId), reqBody, userInformation)
	if err != nil {
		return
	}

	return
}
