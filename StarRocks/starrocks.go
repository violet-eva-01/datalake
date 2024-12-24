// Package StarRocks @author: Violet-Eva @date  : 2024/12/24 @notes :
package StarRocks

import (
	"bytes"
	"fmt"
	"github.com/fatih/color"
	"github.com/violet-eva-01/datalake/conn"
	"github.com/violet-eva-01/datalake/util"
	"gorm.io/gorm"
	"io"
	"net/http"
	"strings"
	"time"
)

type descResult struct {
	Field   string `gorm:"column:field"`
	Type    string `gorm:"column:type"`
	Null    string `gorm:"column:null"`
	Key     string `gorm:"column:key"`
	Default string `gorm:"column:default"`
	Extra   string `gorm:"column:extra"`
}
type StarRocks struct {
	host            string
	feHttpPort      int
	user            string
	passWord        string
	StarRocksSQLDB  *conn.SQLDB
	StarRocksGormDB *gorm.DB
	retryCount      int
	retryInterval   time.Duration
	queryTimeout    time.Duration
}

func NewStarRocksAll(dbName, user, passwd, host string, port, feHttpPort, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime int, opts []string) (*StarRocks, error) {
	sqlDB, err := conn.InitSQLDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	gormDB, err := conn.InitGormDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	return &StarRocks{
		host:            host,
		feHttpPort:      feHttpPort,
		user:            user,
		passWord:        passwd,
		StarRocksGormDB: gormDB,
		StarRocksSQLDB:  sqlDB,
		retryCount:      retryCount,
		retryInterval:   time.Duration(retryInterval) * time.Second,
		queryTimeout:    time.Duration(queryTimeout) * time.Second,
	}, err
}

func NewStarRocks(dbName, user, passwd, host string, opts []string) (*StarRocks, error) {
	port, feHttpPort, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime := 9030, 8030, 3, 10, 1800, 2, 2, 1800
	sqlDB, err := conn.InitSQLDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	gormDB, err := conn.InitGormDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	return &StarRocks{
		host:            host,
		feHttpPort:      feHttpPort,
		user:            user,
		passWord:        passwd,
		StarRocksGormDB: gormDB,
		StarRocksSQLDB:  sqlDB,
		retryCount:      retryCount,
		retryInterval:   time.Duration(retryInterval) * time.Second,
		queryTimeout:    time.Duration(queryTimeout) * time.Second,
	}, err
}

func (s *StarRocks) JsonStreamingLoadToStarRocks(dbName, tblName string, body []byte) (result string, err error) {
	for i := 0; i < s.retryCount; i++ {
		result, err = s.jsonStreamingLoadToStarRocks(dbName, tblName, body)
		if err != nil {
			if i != s.retryCount-1 {
				time.Sleep(s.retryInterval)
				continue
			} else {
				return
			}
		} else {
			return
		}
	}
	return
}

func (s *StarRocks) jsonStreamingLoadToStarRocks(dbName, tblName string, body []byte) (result string, err error) {
	var (
		dr       []descResult
		columns  []string
		req      *http.Request
		resp     *http.Response
		headers  = make(map[string]string)
		respBody []byte
	)
	if s.queryTimeout < 1 {
		s.queryTimeout = 1800 * time.Second
	}

	descSQL := fmt.Sprintf("desc %s.%s", dbName, tblName)
	err = s.StarRocksGormDB.Raw(descSQL).Find(&dr).Error
	if err != nil {
		return
	}

	for _, row := range dr {
		tmpColumn := "\"$." + row.Field + "\""
		columns = append(columns, tmpColumn)
	}

	jsonPaths := "[" + strings.Join(columns, ",") + "]"

	labelName := fmt.Sprintf("%s_%s_%s", dbName, tblName, time.Now().Format("20060102150405"))

	req, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:8030/api/%s/%s/_stream_load", s.host, dbName, tblName), bytes.NewBuffer(body))
	if err != nil {
		return
	}

	headers["label"] = labelName
	headers["Expect"] = "100-continue"
	headers["timezone"] = "Asia/Shanghai"
	headers["format"] = "json"
	headers["max_filter_ratio"] = "0"
	headers["strip_outer_array"] = "true"
	headers["ignore_json_size"] = "true"
	headers["jsonpaths"] = jsonPaths

	util.SetRequestHeader(req, headers)
	util.SetRequestBasicAuth(req, s.user, s.passWord)

	resp, err = (&http.Client{
		Timeout: s.queryTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			color.Blue("stream load be url is: %s", req.URL)
			for key, val := range via[0].Header {
				if key == "Authorization" {
					req.Header[key] = val
				}
			}
			return nil
		},
	}).Do(req)

	if err != nil {
		return
	}

	defer resp.Body.Close()
	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	result = string(respBody)
	return
}
