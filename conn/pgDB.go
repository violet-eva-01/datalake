// Package conn @author: Violet-Eva @date  : 2024/12/24 @notes :
package conn

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
	"strings"
	"time"
)

const pgConnUrl = "postgres://%s:%s@%s:%d/%s%s"

type PgDB struct {
	PostgresDB   *pgxpool.Conn
	QueryTimeout int
}

func connPGDB(user, passwd, host, dbname string, port int, opts ...string) (conn *pgxpool.Conn, err error) {
	var (
		config *pgxpool.Config
		pool   *pgxpool.Pool
	)

	if len(opts) < 1 {
		opts = []string{"sslmode=disable", "connect_timeout=10"}
	}
	opt := "?" + strings.Join(opts, "&")
	url := fmt.Sprintf(pgConnUrl, user, passwd, host, port, dbname, opt)
	config, err = pgxpool.ParseConfig(url)
	if err != nil {
		return
	}
	pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return
	}
	conn, err = pool.Acquire(context.Background())
	if err != nil {
		return
	}
	return
}

func InitPGConn(user, passwd, host, dbname string, port, retryTime, retryInterval, queryTimeout int, opts ...string) (pdb *PgDB, err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	var (
		pgConn *pgxpool.Conn
	)

	for i := 0; i < retryTime; i++ {
		pgConn, err = connPGDB(user, passwd, host, dbname, port, opts...)
		if err != nil {
			if i != retryTime-1 {
				time.Sleep(time.Duration(retryInterval) * time.Second)
				continue
			} else {
				color.Red(fmt.Sprintf("connect pg DB failed ,err is %s", err))
				return
			}
		} else {
			pdb = &PgDB{
				PostgresDB:   pgConn,
				QueryTimeout: queryTimeout,
			}
			return
		}
	}

	return
}

func (p *PgDB) ExecQuery(query string, args ...interface{}) (list []map[string]interface{}, err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()
	var (
		rows pgx.Rows
	)
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(p.QueryTimeout)*time.Second)
	defer cancelFunc()
	rows, err = p.PostgresDB.Query(timeout, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	descriptions := rows.FieldDescriptions()

	scanArgs := make([]interface{}, len(descriptions))
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		var values []any
		values, err = rows.Values()
		if err != nil {
			return
		}
		record := make(map[string]interface{})
		for k, v := range values {
			record[descriptions[k].Name] = v
		}
		list = append(list, record)
	}
	return
}

func (p *PgDB) ExecQueryToString(query string, args ...interface{}) (list []map[string]string, err error) {
	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()
	var (
		rows pgx.Rows
	)
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(p.QueryTimeout)*time.Second)
	defer cancelFunc()
	rows, err = p.PostgresDB.Query(timeout, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	descriptions := rows.FieldDescriptions()

	scanArgs := make([]interface{}, len(descriptions))
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		var values []any
		values, err = rows.Values()
		if err != nil {
			return
		}
		record := make(map[string]string)
		for k, v := range values {
			record[descriptions[k].Name] = fmt.Sprintf("%v", v)
		}
		list = append(list, record)
	}
	return
}

func (p *PgDB) ExecQueryToStruct(query string, data any, args ...interface{}) (err error) {
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
		rows  pgx.Rows
		bytes []byte
	)
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(p.QueryTimeout)*time.Second)
	defer cancelFunc()
	rows, err = p.PostgresDB.Query(timeout, query, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	descriptions := rows.FieldDescriptions()
	var list []map[string]interface{}
	scanArgs := make([]interface{}, len(descriptions))
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return
		}
		var values []any
		values, err = rows.Values()
		if err != nil {
			return
		}
		record := make(map[string]interface{})
		for k, v := range values {
			record[descriptions[k].Name] = v
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

	return
}

func (p *PgDB) ExecQueryNoResult(query string, args ...interface{}) (err error) {

	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Duration(p.QueryTimeout)*time.Second)
	defer cancelFunc()
	_, err = p.PostgresDB.Exec(timeout, query, args...)
	if err != nil {
		return
	}

	return
}
