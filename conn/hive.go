// Package conn @author: Violet-Eva @date  : 2024/12/19 @notes :
package conn

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/fatih/color"
	"golang.org/x/exp/rand"
	"os"
	"os/exec"
	"time"
)

const (
	DefaultKrbConfPath  = "/etc/krb5.conf"
	DefaultKinitProgram = "/usr/bin/kinit"
)

type KrbAuth struct {
	KrbConfPath      string
	KinitProgramPath string
	KeyTabFilePath   string
	Principal        string
}

func NewKrbAuth(krbConfPath, kinitProgramPath, keyTabFilePath, principal string) *KrbAuth {
	if krbConfPath == "" {
		krbConfPath = DefaultKrbConfPath
	}
	if kinitProgramPath == "" {
		kinitProgramPath = DefaultKinitProgram
	}
	return &KrbAuth{
		KrbConfPath:      krbConfPath,
		KinitProgramPath: kinitProgramPath,
		KeyTabFilePath:   keyTabFilePath,
		Principal:        principal,
	}
}

func (ka *KrbAuth) kinit() error {
	err := os.Setenv("KRB5_CONFIG", ka.KrbConfPath)
	if err != nil {
		return err
	}
	cmd := exec.Command(ka.KinitProgramPath, "-kt", ka.KeyTabFilePath, ka.Principal)
	return cmd.Run()
}

type Address struct {
	Host string
	Port int
	// binary http
	Mode string
}

type HiveConnInformation struct {
	Addresses []Address
	// KERBEROS NONE NOSASL LDAP CUSTOM DIGEST-MD5
	Auth          string
	Configuration *gohive.ConnectConfiguration
}

func NewHiveConnInformation(addresses []Address, auth string, service string, fetchSize int64, hiveConfig map[string]string, connTimeout time.Duration) *HiveConnInformation {

	configuration := gohive.NewConnectConfiguration()
	configuration.Service = service
	configuration.FetchSize = fetchSize
	configuration.HiveConfiguration = hiveConfig
	configuration.ConnectTimeout = connTimeout

	return &HiveConnInformation{
		Addresses:     addresses,
		Auth:          auth,
		Configuration: configuration,
	}
}

type HiveConn struct {
	KA            *KrbAuth
	HCI           *HiveConnInformation
	Conn          *gohive.Connection
	RetryCount    int
	RetryInterval time.Duration
	QueryTimeOut  int
}

func NewHiveConn(retryCount int, retryInterval time.Duration, timeOut int, information *HiveConnInformation, auth *KrbAuth) *HiveConn {
	return &HiveConn{
		KA:            auth,
		HCI:           information,
		RetryCount:    retryCount,
		RetryInterval: retryInterval,
		QueryTimeOut:  timeOut,
	}
}

func (hc *HiveConn) KerberosAuthentication() error {
	for i := 0; i < hc.RetryCount; i++ {
		err := hc.KA.kinit()
		if err != nil {
			time.Sleep(hc.RetryInterval * time.Second)
			if i == hc.RetryCount-1 {
				err = fmt.Errorf("kinit failed, err is: %s", err)
				color.Red(err.Error())
				return err
			} else {
				continue
			}
		} else {
			break
		}
	}
	return nil
}

func (hc *HiveConn) GetHiveConn() (err error) {

	if hc.HCI.Auth == "KERBEROS" {
		err = hc.KerberosAuthentication()
		if err != nil {
			return
		}
	}

	var hiveServer2Hosts = hc.HCI.Addresses[:]
	for j := 0; j < hc.RetryCount; j++ {
		i := rand.Intn(len(hiveServer2Hosts))
		hiveServer2Host := hiveServer2Hosts[i]
		hiveServer2Hosts = append(hiveServer2Hosts[:i], hiveServer2Hosts[i+1:]...)
		hc.Conn, err = hc.HCI.getHiveConn(hiveServer2Host)
		if err != nil {
			time.Sleep(hc.RetryInterval * time.Second)
			continue
		} else {
			err = fmt.Errorf("connect HS2 failed ,err is %s", err)
			color.Red(err.Error())
			return err
		}
	}

	return
}

// getHiveConn 获取Hive连接
func (hci *HiveConnInformation) getHiveConn(address Address) (*gohive.Connection, error) {
	now := time.Now()
	defer func() {
		color.Blue("connected to HS2[%s:%d], elapsed time: %s", address.Host, address.Port, time.Since(now).String())
	}()

	hiveConn, err := gohive.Connect(address.Host, address.Port, hci.Auth, hci.Configuration)

	return hiveConn, err
}

func (hc *HiveConn) ExecQuery(query string) ([]map[string]interface{}, error) {
	cur := hc.Conn.Cursor()
	defer cur.Close()
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeOut)*time.Second)
	defer cancelFunc()
	cur.Exec(ctx, query)
	if cur.Err != nil {
		return nil, cur.Err
	}
	var list []map[string]interface{}
	for cur.HasMore(ctx) {
		rowsM := cur.RowMap(ctx)
		if cur.Err != nil {
			return nil, cur.Err
		}
		list = append(list, rowsM)
	}
	return list, nil
}

// ExecQueryToStruct
// @Description:
// @param query
// @param data pointer
// @return []map[string]interface{}
// @return error
func (hc *HiveConn) ExecQueryToStruct(query string, data any) error {
	cur := hc.Conn.Cursor()
	defer cur.Close()
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeOut)*time.Second)
	defer cancelFunc()
	cur.Exec(ctx, query)
	if cur.Err != nil {
		return cur.Err
	}
	var list []map[string]interface{}
	for cur.HasMore(ctx) {
		rowsM := cur.RowMap(ctx)
		if cur.Err != nil {
			return cur.Err
		}
		list = append(list, rowsM)
	}

	marshal, err := json.Marshal(list)
	if err != nil {
		return err
	}

	err = json.Unmarshal(marshal, &data)
	if err != nil {
		return err
	}

	return nil
}

func (hc *HiveConn) ExecQueryNoResult(query string) error {
	cur := hc.Conn.Cursor()
	defer cur.Close()
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeOut)*time.Second)
	defer cancelFunc()
	cur.Exec(ctx, query)
	return cur.Err
}
