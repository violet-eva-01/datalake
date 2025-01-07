// Package StarRocks @author: Violet-Eva @date  : 2024/12/24 @notes :
package StarRocks

import (
	"bytes"
	"encoding/json"
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
	host          string
	feHttpPort    int
	user          string
	passWord      string
	SQLDB         *conn.SQLDB
	GormDB        *gorm.DB
	retryCount    int
	retryInterval time.Duration
	queryTimeout  time.Duration
}

func NewStarRocksAll(dbName, user, passwd, host string, port, feHttpPort, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime int, opts ...string) (*StarRocks, error) {
	sqlDB, err := conn.InitSQLDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	gormDB, err := conn.InitGormDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	return &StarRocks{
		host:          host,
		feHttpPort:    feHttpPort,
		user:          user,
		passWord:      passwd,
		GormDB:        gormDB,
		SQLDB:         sqlDB,
		retryCount:    retryCount,
		retryInterval: time.Duration(retryInterval) * time.Second,
		queryTimeout:  time.Duration(queryTimeout) * time.Second,
	}, err
}

func NewStarRocks(dbName, user, passwd, host string, timeout int, opts ...string) (*StarRocks, error) {
	port, feHttpPort, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime := 9030, 8030, 3, 10, timeout, 2, 2, timeout
	sqlDB, err := conn.InitSQLDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	gormDB, err := conn.InitGormDB("mysql", dbName, user, passwd, host, port, retryCount, retryInterval, maxIdleConn, maxOpenConn, maxLifetime, opts...)
	if err != nil {
		return nil, err
	}
	return &StarRocks{
		host:          host,
		feHttpPort:    feHttpPort,
		user:          user,
		passWord:      passwd,
		GormDB:        gormDB,
		SQLDB:         sqlDB,
		retryCount:    retryCount,
		retryInterval: time.Duration(retryInterval) * time.Second,
		queryTimeout:  time.Duration(queryTimeout) * time.Second,
	}, err
}

func (s *StarRocks) JsonStreamingLoadToStarRocks(dbName, tblName string, body []byte) (err error) {
	for i := 0; i < s.retryCount; i++ {
		err = s.jsonStreamingLoadToStarRocks(dbName, tblName, body)
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

type streamLoadResult struct {
	TxnId                  int    `json:"TxnId"`
	Label                  string `json:"Label"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int    `json:"NumberTotalRows"`
	NumberLoadedRows       int    `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int64  `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	BeginTxnTimeMs         int    `json:"BeginTxnTimeMs"`
	StreamLoadPlanTimeMs   int    `json:"StreamLoadPlanTimeMs"`
	ReadDataTimeMs         int    `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
}

func (s *StarRocks) jsonStreamingLoadToStarRocks(dbName, tblName string, body []byte) (err error) {
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
	err = s.GormDB.Raw(descSQL).Find(&dr).Error
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
	var slr streamLoadResult
	err = json.Unmarshal(respBody, &slr)
	if err != nil {
		return
	}
	if strings.ToLower(slr.Status) != "success" {
		err = fmt.Errorf("%s", slr.Message)
	}
	return
}

func (s *StarRocks) Close() error {
	err := s.SQLDB.Close()
	return err
}
