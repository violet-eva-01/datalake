package StarRocks

import (
	"github.com/violet-eva-01/datalake/util"
	"strings"
)

type Permission int

const (
	Grant Permission = 0 + iota
	Node
	CreateResourceGroup
	CreateResource
	CreateExternalCatalog
	Plugin
	Repository
	BlackList
	File
	Operate
	CreateGlobalFunction
	CreateStorageVolume
	Usage
	All
	Impersonate
	Apply
	CreateMaskingPolicy
	CreateRowAccessPolicy
	CreatePipe
	CreateWarehouse
	Security
	CreateDatabase
	CreateTable
	CreateView
	CreateFunction
	CreateMaterializedView
	Refresh
	Select
	Alter
	Insert
	Create
	Drop
	Update
	Delete
)

var starRocksPermissionNames = []string{
	"GRANT",
	"NODE",
	"CREATE RESOURCE GROUP",
	"CREATE RESOURCE",
	"CREATE EXTERNAL CATALOG",
	"PLUGIN",
	"REPOSITORY",
	"BLACKLIST",
	"FILE",
	"OPERATE",
	"CREATE GLOBAL FUNCTION",
	"CREATE STORAGE VOLUME",
	"USAGE",
	"ALL",
	"IMPERSONATE",
	"APPLY",
	"CREATE MASKING POLICY",
	"CREATE ROW ACCESS POLICY",
	"CREATE PIPE",
	"CREATE WAREHOUSE",
	"SECURITY",
	"CREATE DATABASE",
	"CREATE TABLE",
	"CREATE VIEW",
	"CREATE FUNCTION",
	"CREATE MATERIALIZED VIEW",
	"REFRESH",
	"SELECT",
	"ALTER",
	"INSERT",
	"CREATE",
	"DROP",
	"UPDATE",
	"DELETE",
}

func ParsePermissionName(str string) Permission {

	index := util.FindIndex(strings.ToUpper(str), starRocksPermissionNames)
	if index == -1 {
		return -1
	} else {
		return Permission(index)
	}

}

func (sp Permission) String() string {

	if sp >= Grant && sp <= Delete {
		return starRocksPermissionNames[sp]
	}

	return "nil"
}

func (sp Permission) RegexpString() string {
	if sp >= Grant && sp <= Delete {
		return strings.ReplaceAll(starRocksPermissionNames[sp], " ", "\\s+")
	}
	return "nil"
}

type PermissionType int

const (
	SystemType PermissionType = 0 + iota
	WarehouseType
	ResourceGroupType
	ResourceType
	UserType
	GlobalFunctionType
	FunctionType
	CatalogType
	StorageVolumeType
	MaskingPolicyType
	RowAccessPolicyType
	DatabaseType
	TableType
	MaterializedViewType
	ViewType
)

var permissionTypeNames = []string{
	"SYSTEM",
	"WAREHOUSE",
	"RESOURCE GROUP",
	"RESOURCE",
	"USER",
	"GLOBAL FUNCTION",
	"FUNCTION",
	"CATALOG",
	"STORAGE VOLUME",
	"MASKING POLICY",
	"ROW ACCESS POLICY",
	"DATABASE",
	"TABLE",
	"MATERIALIZED VIEW",
	"VIEW",
}

func (spt PermissionType) String() string {
	if spt >= SystemType && spt <= ViewType {
		return permissionTypeNames[spt]
	}
	return "nil"
}

func (spt PermissionType) RegexpString() string {
	if spt >= SystemType && spt <= ViewType {
		return strings.ReplaceAll(permissionTypeNames[spt], " ", "\\s+")
	}
	return "nil"
}

func ParsePermissionTypeName(str string) PermissionType {
	index := util.FindIndex(strings.ToUpper(str), permissionTypeNames)
	if index == -1 {
		return -1
	} else {
		return PermissionType(index)
	}
}
