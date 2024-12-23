// Package tencent @author: Violet-Eva @date  : 2024/12/17 @notes :
package tencent

import (
	"time"
)

type CosInformation struct {
	Name     string `gorm:"column:name" json:"name"`
	Atime    string `gorm:"column:atime" json:"atime"`
	Mtime    string `gorm:"column:mtime" json:"mtime"`
	Ctime    string `gorm:"column:ctime" json:"ctime"`
	SizeByte int64  `gorm:"column:size_byte" json:"size_byte"`
	// DIR FILE
	Type     string `gorm:"column:type" json:"type"`
	Acl      string `gorm:"column:acl" json:"acl"`
	User     string `gorm:"column:user" json:"user"`
	Group    string `gorm:"column:group" json:"group"`
	FileType string `gorm:"column:file_type" json:"file_type"`
	DT       string `gorm:"column:dt" json:"dt"`
}

type CosInformationParse struct {
	Name             string    `gorm:"column:name" json:"name"`
	PathLevel        int       `gorm:"column:path_level" json:"path_level"`
	ExtendLevel0Name string    `gorm:"column:extend_level0_name" json:"extend_level0_name"`
	ExtendLevel1Name string    `gorm:"column:extend_level1_name" json:"extend_level1_name"`
	ExtendLevel2Name string    `gorm:"column:extend_level2_name" json:"extend_level2_name"`
	ExtendLevel3Name string    `gorm:"column:extend_level3_name" json:"extend_level3_name"`
	ExtendLevel4Name string    `gorm:"column:extend_level4_name" json:"extend_level4_name"`
	ExtendLevel5Name string    `gorm:"column:extend_level5_name" json:"extend_level5_name"`
	ExtendLevel6Name string    `gorm:"column:extend_level6_name" json:"extend_level6_name"`
	TableName        string    `gorm:"column:table_name" json:"table_name"`
	DBName           string    `gorm:"column:db_name" json:"db_name"`
	TBLName          string    `gorm:"column:tbl_name" json:"tbl_name"`
	Type             string    `gorm:"column:type" json:"type"`
	Atime            time.Time `gorm:"column:atime" json:"atime"`
	Mtime            time.Time `gorm:"column:mtime" json:"mtime"`
	Ctime            time.Time `gorm:"column:ctime" json:"ctime"`
	Size             int64     `gorm:"column:size" json:"size"`
	Acl              string    `gorm:"column:acl" json:"acl"`
	User             string    `gorm:"column:user" json:"user"`
	Group            string    `gorm:"column:group" json:"group"`
	DT               string    `gorm:"column:dt" json:"dt"`
}
