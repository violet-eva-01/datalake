// Package conn @author: Violet-Eva @date  : 2024/12/24 @notes :
package conn

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"reflect"
	"strings"
	"time"
)

type SQLDB struct {
	QueryTimeOut int
	SQLDB        *sql.DB
}

func connSQLDB(dbType, dbName, user, passwd, host string, port, maxIdleConn, maxOpenConn, maxLifetime int, opts ...string) (sqlDB *sql.DB, err error) {

	var (
		db  *gorm.DB
		opt string
	)

	switch dbType {
	case "mysql":
		if len(opts) < 1 {
			opts = []string{"timeout=10s", "charset=utf8mb4"}
		}
		opt = "?" + strings.Join(opts, "&")
		dsn := fmt.Sprintf(mysqlDSN, user, passwd, host, port, dbName, opt)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		})
		if err != nil {
			return
		}
	case "postgres":
		if len(opts) < 1 {
			opts = []string{"sslmode=disable", "connect_timeout=10s"}
		}
		opt = strings.Join(opts, " ")
		dsn := fmt.Sprintf(pgDSN, host, user, passwd, dbName, port, opt)
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		})
		if err != nil {
			return
		}
	default:
		return nil, fmt.Errorf("connDB: unknown db type: %s", dbType)
	}

	if err != nil {
		return
	}

	sqlDB, err = db.DB()
	if err != nil {
		return
	}
	sqlDB.SetMaxIdleConns(maxIdleConn)
	sqlDB.SetMaxOpenConns(maxOpenConn)
	sqlDB.SetConnMaxLifetime(time.Duration(maxLifetime) * time.Second)
	return
}

func InitSQLDB(dbType, dbName, user, passwd, host string, port, retryCount, retryInterval, queryTimeout, maxIdleConn, maxOpenConn, maxLifetime int, opts ...string) (sqlDB *SQLDB, err error) {

	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	var (
		sdb *sql.DB
	)

	for i := 0; i < retryCount; i++ {
		sdb, err = connSQLDB(dbType, dbName, user, passwd, host, port, maxIdleConn, maxOpenConn, maxLifetime, opts...)
		if err != nil {
			if i != retryCount-1 {
				time.Sleep(time.Duration(retryInterval) * time.Second)
				continue
			} else {
				color.Red(fmt.Sprintf("connect %s DB failed ,err is %s", dbType, err))
				return
			}
		} else {
			sqlDB = &SQLDB{
				SQLDB:        sdb,
				QueryTimeOut: queryTimeout,
			}
			return
		}
	}

	return
}

func (s *SQLDB) ExecQuery(query string, args ...interface{}) (list []map[string]interface{}, err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	var (
		rows    *sql.Rows
		columns []string
	)

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(s.QueryTimeOut)*time.Second)
	defer cancelFunc()
	rows, err = s.SQLDB.QueryContext(timeout, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	rawBytes := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(rawBytes))
	for i := range columns {
		scanArgs[i] = &rawBytes[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		record := make(map[string]interface{})
		for index, value := range rawBytes {
			record[columns[index]] = string(value)
		}
		list = append(list, record)
	}
	return list, nil
}

func (s *SQLDB) ExecQueryBatchProcessing(query string, batchSize int, function ...func(input []map[string]interface{}) error) (err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	var (
		rows    *sql.Rows
		columns []string
	)

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(s.QueryTimeOut)*time.Second)
	defer cancelFunc()
	rows, err = s.SQLDB.QueryContext(timeout, query)
	if err != nil {
		return
	}
	defer rows.Close()
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	var list []map[string]interface{}
	rawBytes := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(rawBytes))
	for i := range columns {
		scanArgs[i] = &rawBytes[i]
	}
	rowCount := 0
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		record := make(map[string]interface{})
		for index, value := range rawBytes {
			record[columns[index]] = string(value)
		}
		list = append(list, record)
		rowCount++
		if rowCount%batchSize == 0 {
			for _, fun := range function {
				err = fun(list)
				if err != nil {
					return
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

func (s *SQLDB) ExecQueryToString(query string, args ...interface{}) (list []map[string]string, err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	var (
		rows    *sql.Rows
		columns []string
	)

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(s.QueryTimeOut)*time.Second)
	defer cancelFunc()
	rows, err = s.SQLDB.QueryContext(timeout, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	rawBytes := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(rawBytes))
	for i := range columns {
		scanArgs[i] = &rawBytes[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		record := make(map[string]string)
		for index, value := range rawBytes {
			record[columns[index]] = string(value)
		}
		list = append(list, record)
	}
	return
}

func (s *SQLDB) ExecQueryToStruct(query string, data any, args ...interface{}) (err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	valueOf := reflect.ValueOf(data)
	if valueOf.Kind() != reflect.Ptr {
		return fmt.Errorf("data is not a pointer")
	}

	var (
		rows    *sql.Rows
		columns []string
		bytes   []byte
	)

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(s.QueryTimeOut)*time.Second)
	defer cancelFunc()
	rows, err = s.SQLDB.QueryContext(timeout, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	rawBytes := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(rawBytes))
	for i := range columns {
		scanArgs[i] = &rawBytes[i]
	}

	var list []map[string]string
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		record := make(map[string]string)
		for index, value := range rawBytes {
			record[columns[index]] = string(value)
		}
		list = append(list, record)
	}
	bytes, err = json.Marshal(list)
	if err != nil {
		return
	}
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		return
	}
	return nil
}

func (s *SQLDB) ExecQueryNoResult(query string, args ...interface{}) (err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(s.QueryTimeOut)*time.Second)
	defer cancelFunc()
	_, err = s.SQLDB.ExecContext(timeout, query, args...)
	if err != nil {
		return
	}
	return
}

func (s *SQLDB) Close() (err error) {
	err = s.SQLDB.Close()
	return
}
