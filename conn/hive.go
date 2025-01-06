// Package conn @author: Violet-Eva @date  : 2024/12/19 @notes :
package conn

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beltran/gohive"
	"github.com/fatih/color"
	"golang.org/x/exp/rand"
	"reflect"
	"time"
)

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
	ConnTimeout   time.Duration
}

func NewHiveConnInformation(addresses []Address, auth string, service string, fetchSize int64, hiveConfig map[string]string, connTimeout time.Duration) *HiveConnInformation {

	configuration := gohive.NewConnectConfiguration()
	configuration.Service = service
	configuration.FetchSize = fetchSize
	configuration.HiveConfiguration = hiveConfig

	return &HiveConnInformation{
		Addresses:     addresses,
		Auth:          auth,
		Configuration: configuration,
		ConnTimeout:   connTimeout,
	}
}

type HiveConn struct {
	KA            *KrbAuth
	HCI           *HiveConnInformation
	Conn          *gohive.Connection
	RetryCount    int
	RetryInterval time.Duration
	QueryTimeout  int
}

func NewHiveConn(retryCount int, retryInterval time.Duration, queryTimeout int, information *HiveConnInformation, auth *KrbAuth) *HiveConn {
	return &HiveConn{
		KA:            auth,
		HCI:           information,
		RetryCount:    retryCount,
		RetryInterval: retryInterval,
		QueryTimeout:  queryTimeout,
	}
}

func (hc *HiveConn) kerberosAuthentication() error {
	for i := 0; i < hc.RetryCount; i++ {
		err := hc.KA.Kinit()
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

type conn struct {
	conn *gohive.Connection
	err  error
}

func (hci *HiveConnInformation) hiveConn(address Address) chan conn {
	ch := make(chan conn)
	go func() {
		hiveConn, err := gohive.Connect(address.Host, address.Port, hci.Auth, hci.Configuration)
		ch <- conn{hiveConn, err}
	}()
	return ch
}

// getHiveConn 获取Hive连接
func (hci *HiveConnInformation) getHiveConn(address Address) (*gohive.Connection, error) {
	now := time.Now()
	defer func() {
		color.Blue("connected to HS2[%s:%d], elapsed time: %s", address.Host, address.Port, time.Since(now).String())
	}()

	ch := hci.hiveConn(address)

	timeOut := time.After(hci.ConnTimeout * time.Second)

	select {
	case c := <-ch:
		if c.err != nil {
			color.Red(c.err.Error())
			return nil, c.err
		} else {
			return c.conn, nil
		}
	case <-timeOut:
		err := fmt.Errorf("connect timeout")
		color.Red(err.Error())
		return nil, err
	}
}

func (hc *HiveConn) GetHiveConn() (err error) {

	if hc.HCI.Auth == "KERBEROS" {
		err = hc.kerberosAuthentication()
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
			if j < hc.RetryCount-1 {
				time.Sleep(hc.RetryInterval * time.Second)
				continue
			} else {
				color.Red(fmt.Sprintf("connect HS2 failed ,err is %s", err))
				return
			}
		} else {
			return
		}
	}

	return
}

func (hc *HiveConn) ExecQueryBatchSize(query string, batchSize int, function ...func(input []map[string]interface{}) error) (err error) {

	cur := hc.Conn.Cursor()
	defer cur.Close()
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeout)*time.Second)
	defer cancelFunc()
	cur.Exec(ctx, query)
	if cur.Err != nil {
		return cur.Err
	}
	var list []map[string]interface{}
	rowCount := 0
	for cur.HasMore(ctx) {
		rowsM := cur.RowMap(ctx)
		if cur.Err != nil {
			return cur.Err
		}
		list = append(list, rowsM)
		rowCount++
		if rowCount%batchSize == 0 {
			for _, fun := range function {
				err = fun(list)
				if err != nil {
					return err
				}
			}
			list = list[:0]
		}
	}
	if len(list) > 0 {
		for _, fun := range function {
			err = fun(list)
			if err != nil {
				return
			}
		}
	}
	return
}

func (hc *HiveConn) ExecQuery(query string) ([]map[string]interface{}, error) {
	cur := hc.Conn.Cursor()
	defer cur.Close()
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeout)*time.Second)
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
	valueOf := reflect.ValueOf(data)
	if valueOf.Kind() != reflect.Ptr {
		return fmt.Errorf("data is not a pointer")
	}
	cur := hc.Conn.Cursor()
	defer cur.Close()
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeout)*time.Second)
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
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(hc.QueryTimeout)*time.Second)
	defer cancelFunc()
	cur.Exec(ctx, query)
	return cur.Err
}

func (hc *HiveConn) Close() (err error) {
	err = hc.Conn.Close()
	return
}
