// Package conn @author: Violet-Eva @date  : 2024/12/24 @notes :
package conn

import (
	"database/sql"
	"fmt"
	"github.com/fatih/color"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"time"
)

const (
	mysqlDSN = "%s:%s@tcp(%s:%d)/%s%s"
	pgDSN    = "host=%s user=%s password=%s dbname=%s port=%d %s"
)

func connGormDB(dbType, dbName, user, passwd, host string, port, maxIdleConn, maxOpenConn, maxLifetime int, opts ...string) (db *gorm.DB, err error) {

	var (
		sqlDB *sql.DB
		opt   string
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

func InitGormDB(dbType, dbName, user, passwd, host string, port, retryCount, retryInterval, maxIdleConn, maxOpenConn, maxLifetime int, opts ...string) (gormDB *gorm.DB, err error) {

	defer func() {
		if result := recover(); result != nil {
			err = fmt.Errorf("panic: %v", result)
		}
	}()

	for i := 0; i < retryCount; i++ {
		gormDB, err = connGormDB(dbType, dbName, user, passwd, host, port, maxIdleConn, maxOpenConn, maxLifetime, opts...)
		if err != nil {
			if i != retryCount-1 {
				time.Sleep(time.Duration(retryInterval) * time.Second)
				continue
			} else {
				color.Red(fmt.Sprintf("connect %s DB failed ,err is %s", dbType, err))
				return
			}
		} else {
			return
		}
	}
	return
}
