// Package ranger @author: Violet-Eva @date  : 2024/11/22 @notes :
package ranger

import "time"

type DatabaseResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type TableResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type ColumnResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type GlobalResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type HiveServiceResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type UDFResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type URLResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type BucketResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type MountPointResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type PathResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type QueueResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

type KeyNameResource struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Accesses
// @Description: 除加密解密相关权限的其他权限
type Accesses struct {
	Type      string `json:"type"`
	IsAllowed bool   `json:"isAllowed"`
}

// Conditions
// @Description: 用户自定义限制规则
type Conditions struct {
	Values []string `json:"values"`
	Type   string   `json:"type"`
}

// PolicyItems
// @Description: 授权
type PolicyItems struct {
	Users         []string     `json:"users"`
	Accesses      []Accesses   `json:"accesses"`
	Groups        []string     `json:"groups"`
	Roles         []string     `json:"roles"`
	Conditions    []Conditions `json:"conditions"`
	DelegateAdmin bool         `json:"delegateAdmin"`
}

// AllowExceptions
// @Description: 除外授权
type AllowExceptions struct {
	Users         []string     `json:"users"`
	Accesses      []Accesses   `json:"accesses"`
	Groups        []string     `json:"groups"`
	Roles         []string     `json:"roles"`
	Conditions    []Conditions `json:"conditions"`
	DelegateAdmin bool         `json:"delegateAdmin"`
}

// DenyPolicyItems
// @Description: 回收权限
type DenyPolicyItems struct {
	Users         []string     `json:"users"`
	Accesses      []Accesses   `json:"accesses"`
	Groups        []string     `json:"groups"`
	Roles         []string     `json:"roles"`
	Conditions    []Conditions `json:"conditions"`
	DelegateAdmin bool         `json:"delegateAdmin"`
}

// DenyExceptions
// @Description: 除外回收权限
type DenyExceptions struct {
	Users         []string     `json:"users"`
	Accesses      []Accesses   `json:"accesses"`
	Groups        []string     `json:"groups"`
	Roles         []string     `json:"roles"`
	Conditions    []Conditions `json:"conditions"`
	DelegateAdmin bool         `json:"delegateAdmin"`
}

// ValiditySchedules
// @Description: 有效时间
type ValiditySchedules struct {
	StartTime   string `json:"startTime"`
	EndTime     string `json:"endTime"`
	TimeZone    string `json:"timeZone"`
	Recurrences []struct {
		Interval struct {
		} `json:"interval"`
		Schedule struct {
		} `json:"schedule"`
	} `json:"recurrences"`
}

// DataMaskInfo
// @Description: 加密解密相关权限
type DataMaskInfo struct {
	ConditionExpr string `json:"conditionExpr"`
	DataMaskType  string `json:"dataMaskType"`
	ValueExpr     string `json:"valueExpr"`
}

// DataMaskPolicyItems
// @Description: 加密 & 授予解密权限
type DataMaskPolicyItems struct {
	DataMaskInfo  DataMaskInfo `json:"dataMaskInfo"`
	Users         []string     `json:"users"`
	Accesses      []Accesses   `json:"accesses"`
	Groups        []string     `json:"groups"`
	Roles         []string     `json:"roles"`
	Conditions    []Conditions `json:"conditions"`
	DelegateAdmin bool         `json:"delegateAdmin"`
}

// RowFilterPolicyItems
// @Description: 行级过滤限制
type RowFilterPolicyItems struct {
	RowFilterInfo struct {
		FilterExpr string `json:"filterExpr"`
	} `json:"rowFilterInfo"`
	Users         []string     `json:"users"`
	Accesses      []Accesses   `json:"accesses"`
	Groups        []string     `json:"groups"`
	Roles         []string     `json:"roles"`
	Conditions    []Conditions `json:"conditions"`
	DelegateAdmin bool         `json:"delegateAdmin"`
}

type Resource struct {
	// hive service 相关
	Database    DatabaseResource    `json:"database,omitempty"`
	Table       TableResource       `json:"table,omitempty"`
	Column      ColumnResource      `json:"column,omitempty"`
	Global      GlobalResource      `json:"global,omitempty"`
	HiveService HiveServiceResource `json:"hiveservice,omitempty"`
	Udf         UDFResource         `json:"udf,omitempty"`
	Url         URLResource         `json:"url,omitempty"`
	// cos & hdfs & chdfs service 相关
	Bucket     BucketResource     `json:"bucket,omitempty"`
	MountPoint MountPointResource `json:"mountpoint,omitempty"`
	Path       PathResource       `json:"path,omitempty"`
	// yarn service 相关
	Queue QueueResource `json:"queue,omitempty"`
	// kms service 相关
	KeyName KeyNameResource `json:"keyname,omitempty"`
}

// PolicyBody
// @Description: ranger policy 和 hdfs hive yarn cos service 相关的 body
type PolicyBody struct {
	Id         int    `json:"id"`
	Guid       string `json:"guid"`
	IsEnabled  bool   `json:"isEnabled"`
	Version    int    `json:"version"`
	Service    string `json:"service"`
	Name       string `json:"name"`
	PolicyType int    `json:"policyType"`
	// PolicyPriority
	// @Description: 0 normal 1 overrides
	PolicyPriority  int               `json:"policyPriority"`
	Description     string            `json:"description"`
	IsAuditEnabled  bool              `json:"isAuditEnabled"`
	Resources       Resource          `json:"resources"`
	PolicyItems     []PolicyItems     `json:"policyItems,omitempty"`
	DenyPolicyItems []DenyPolicyItems `json:"denyPolicyItems,omitempty"`
	// IsDenyAllElse
	// @Description: 拒绝所有其他访问
	IsDenyAllElse   bool              `json:"isDenyAllElse"`
	AllowExceptions []AllowExceptions `json:"allowExceptions,omitempty"`
	DenyExceptions  []DenyExceptions  `json:"denyExceptions,omitempty"`
	// DataMaskPolicyItems
	// @Description: 加密解密时单独使用
	DataMaskPolicyItems []DataMaskPolicyItems `json:"dataMaskPolicyItems,omitempty"`
	// RowFilterPolicyItems
	// @Description: 行加密单独使用
	RowFilterPolicyItems []RowFilterPolicyItems `json:"rowFilterPolicyItems,omitempty"`
	ServiceType          string                 `json:"serviceType"`
	Options              struct {
		// PolicyValiditySchedules
		// @Description: 根据有效时间自动生成
		PolicyValiditySchedules string `json:"POLICY_VALIDITY_SCHEDULES,omitempty"`
	} `json:"options"`
	ValiditySchedules []ValiditySchedules `json:"validitySchedules,omitempty"`
	PolicyLabels      []string            `json:"policyLabels"`
	ZoneName          string              `json:"zoneName"`
}

type ServiceDef struct {
	Id          int    `json:"id"`
	Guid        string `json:"guid"`
	IsEnabled   bool   `json:"isEnabled"`
	CreateTime  int64  `json:"createTime"`
	UpdateTime  int64  `json:"updateTime"`
	Version     int    `json:"version"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	ImplClass   string `json:"implClass"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Options     struct {
		EnableDenyAndExceptionsInPolicies string `json:"enableDenyAndExceptionsInPolicies"`
		UiPages                           string `json:"ui.pages,omitempty"`
	} `json:"options"`
	Configs []struct {
		ItemId            int    `json:"itemId"`
		Name              string `json:"name"`
		Type              string `json:"type"`
		SubType           string `json:"subType,omitempty"`
		Mandatory         bool   `json:"mandatory"`
		ValidationRegEx   string `json:"validationRegEx,omitempty"`
		ValidationMessage string `json:"validationMessage,omitempty"`
		UiHint            string `json:"uiHint,omitempty"`
		Label             string `json:"label,omitempty"`
		DefaultValue      string `json:"defaultValue,omitempty"`
	} `json:"configs"`
	Resources []struct {
		ItemId             int    `json:"itemId"`
		Name               string `json:"name"`
		Type               string `json:"type"`
		Level              int    `json:"level"`
		Mandatory          bool   `json:"mandatory"`
		LookupSupported    bool   `json:"lookupSupported"`
		RecursiveSupported bool   `json:"recursiveSupported"`
		ExcludesSupported  bool   `json:"excludesSupported"`
		Matcher            string `json:"matcher,omitempty"`
		MatcherOptions     struct {
			WildCard          string `json:"wildCard,omitempty"`
			IgnoreCase        string `json:"ignoreCase,omitempty"`
			PathSeparatorChar string `json:"pathSeparatorChar,omitempty"`
		} `json:"matcherOptions"`
		ValidationRegEx        string   `json:"validationRegEx,omitempty"`
		ValidationMessage      string   `json:"validationMessage,omitempty"`
		UiHint                 string   `json:"uiHint,omitempty"`
		Label                  string   `json:"label"`
		Description            string   `json:"description"`
		AccessTypeRestrictions []string `json:"accessTypeRestrictions"`
		IsValidLeaf            bool     `json:"isValidLeaf"`
		Parent                 string   `json:"parent,omitempty"`
	} `json:"resources"`
	AccessTypes []struct {
		ItemId        int      `json:"itemId"`
		Name          string   `json:"name"`
		Label         string   `json:"label"`
		ImpliedGrants []string `json:"impliedGrants"`
	} `json:"accessTypes"`
	PolicyConditions []struct {
		ItemId           int    `json:"itemId"`
		Name             string `json:"name"`
		Evaluator        string `json:"evaluator"`
		EvaluatorOptions struct {
			ScriptTemplate string `json:"scriptTemplate,omitempty"`
			EngineName     string `json:"engineName,omitempty"`
			UiIsMultiline  string `json:"ui.isMultiline,omitempty"`
		} `json:"evaluatorOptions"`
		ValidationRegEx   string `json:"validationRegEx,omitempty"`
		ValidationMessage string `json:"validationMessage,omitempty"`
		UiHint            string `json:"uiHint,omitempty"`
		Label             string `json:"label"`
		Description       string `json:"description"`
	} `json:"policyConditions"`
	ContextEnrichers []struct {
		ItemId          int    `json:"itemId"`
		Name            string `json:"name"`
		Enricher        string `json:"enricher"`
		EnricherOptions struct {
			TagRetrieverClassName       string `json:"tagRetrieverClassName"`
			TagRefresherPollingInterval string `json:"tagRefresherPollingInterval"`
		} `json:"enricherOptions"`
	} `json:"contextEnrichers"`
	Enums []struct {
		ItemId   int    `json:"itemId"`
		Name     string `json:"name"`
		Elements []struct {
			ItemId int    `json:"itemId"`
			Name   string `json:"name"`
			Label  string `json:"label"`
		} `json:"elements"`
		DefaultIndex int `json:"defaultIndex"`
	} `json:"enums"`
	DataMaskDef struct {
		MaskTypes []struct {
			ItemId          int    `json:"itemId"`
			Name            string `json:"name"`
			Label           string `json:"label"`
			Description     string `json:"description"`
			Transformer     string `json:"transformer,omitempty"`
			DataMaskOptions struct {
			} `json:"dataMaskOptions"`
		} `json:"maskTypes"`
		AccessTypes []struct {
			ItemId        int           `json:"itemId"`
			Name          string        `json:"name"`
			Label         string        `json:"label"`
			ImpliedGrants []interface{} `json:"impliedGrants"`
		} `json:"accessTypes"`
		Resources []struct {
			ItemId             int    `json:"itemId"`
			Name               string `json:"name"`
			Type               string `json:"type"`
			Level              int    `json:"level"`
			Mandatory          bool   `json:"mandatory"`
			LookupSupported    bool   `json:"lookupSupported"`
			RecursiveSupported bool   `json:"recursiveSupported"`
			ExcludesSupported  bool   `json:"excludesSupported"`
			Matcher            string `json:"matcher"`
			MatcherOptions     struct {
				WildCard               string `json:"wildCard"`
				IgnoreCase             string `json:"ignoreCase"`
				IsValidLeaf            string `json:"__isValidLeaf,omitempty"`
				AccessTypeRestrictions string `json:"__accessTypeRestrictions,omitempty"`
			} `json:"matcherOptions"`
			ValidationRegEx        string   `json:"validationRegEx"`
			ValidationMessage      string   `json:"validationMessage"`
			UiHint                 string   `json:"uiHint"`
			Label                  string   `json:"label"`
			Description            string   `json:"description"`
			AccessTypeRestrictions []string `json:"accessTypeRestrictions"`
			IsValidLeaf            bool     `json:"isValidLeaf"`
			Parent                 string   `json:"parent,omitempty"`
		} `json:"resources"`
	} `json:"dataMaskDef"`
	RowFilterDef struct {
		AccessTypes []struct {
			ItemId        int           `json:"itemId"`
			Name          string        `json:"name"`
			Label         string        `json:"label"`
			ImpliedGrants []interface{} `json:"impliedGrants"`
		} `json:"accessTypes"`
		Resources []struct {
			ItemId             int    `json:"itemId"`
			Name               string `json:"name"`
			Type               string `json:"type"`
			Level              int    `json:"level"`
			Mandatory          bool   `json:"mandatory"`
			LookupSupported    bool   `json:"lookupSupported"`
			RecursiveSupported bool   `json:"recursiveSupported"`
			ExcludesSupported  bool   `json:"excludesSupported"`
			Matcher            string `json:"matcher"`
			MatcherOptions     struct {
				WildCard   string `json:"wildCard"`
				IgnoreCase string `json:"ignoreCase"`
			} `json:"matcherOptions"`
			ValidationRegEx        string   `json:"validationRegEx"`
			ValidationMessage      string   `json:"validationMessage"`
			UiHint                 string   `json:"uiHint"`
			Label                  string   `json:"label"`
			Description            string   `json:"description"`
			AccessTypeRestrictions []string `json:"accessTypeRestrictions"`
			IsValidLeaf            bool     `json:"isValidLeaf"`
			Parent                 string   `json:"parent,omitempty"`
		} `json:"resources"`
	} `json:"rowFilterDef"`
	CreatedBy string `json:"createdBy,omitempty"`
	UpdatedBy string `json:"updatedBy,omitempty"`
}

type PluginsDefinitions struct {
	StartIndex  int          `json:"startIndex"`
	PageSize    int          `json:"pageSize"`
	TotalCount  int          `json:"totalCount"`
	ResultSize  int          `json:"resultSize"`
	QueryTimeMS int64        `json:"queryTimeMS"`
	ServiceDefs []ServiceDef `json:"serviceDefs"`
}

var tencentUserInformationIndex = make(map[string]int)

type VXUser struct {
	Id              int       `json:"id"`
	CreateDate      time.Time `json:"createDate"`
	UpdateDate      time.Time `json:"updateDate"`
	EmailAddress    string    `json:"emailAddress,omitempty"`
	Owner           string    `json:"owner,omitempty"`
	UpdatedBy       string    `json:"updatedBy,omitempty"`
	Name            string    `json:"name"`
	Password        string    `json:"password,omitempty"`
	Description     string    `json:"description"`
	GroupIdList     []int     `json:"groupIdList"`
	GroupNameList   []string  `json:"groupNameList"`
	Status          int       `json:"status"`
	IsVisible       int       `json:"isVisible"`
	UserSource      int       `json:"userSource"`
	UserRoleList    []string  `json:"userRoleList"`
	OtherAttributes string    `json:"otherAttributes,omitempty"`
	SyncSource      string    `json:"syncSource,omitempty"`
	FirstName       string    `json:"firstName,omitempty"`
	LastName        string    `json:"lastName,omitempty"`
}

type XUsers struct {
	StartIndex  int      `json:"startIndex"`
	PageSize    int      `json:"pageSize"`
	TotalCount  int      `json:"totalCount"`
	ResultSize  int      `json:"resultSize"`
	SortType    string   `json:"sortType"`
	SortBy      string   `json:"sortBy"`
	QueryTimeMS int64    `json:"queryTimeMS"`
	VXUsers     []VXUser `json:"vXUsers"`
}

type ServiceType int

const (
	HiveServiceType ServiceType = iota
	HdfsServiceType
	CosServiceType
	YarnServiceType
	kmsServiceType
	ChdfsServiceType
)

var serviceTypeName = []string{
	"hive",
	"hdfs",
	"cos",
	"yarn",
	"kms",
	"chdfs",
}

func (st ServiceType) String() string {
	if st >= HiveServiceType && st <= ChdfsServiceType {
		return serviceTypeName[st]
	}
	return "unknown service type"
}

type ServiceTypeId struct {
	ServiceType   ServiceType `json:"serviceType"`
	ServiceTypeId int         `json:"serviceTypeId"`
}

type ObjectType int

const (
	HiveService ObjectType = iota
	Url
	GlobalUdf
	Udf
	Database
	Table
	Column
	Masking
	RowFilter
	Hdfs
	Yarn
	Cos
	Chdfs
)

var objectTypeName = []string{
	"HIVE SERVICE",
	"URL",
	"GLOBAL UDF",
	"UDF",
	"DATABASE",
	"TABLE",
	"COLUMN",
	"MASKING",
	"ROW FILTER",
	"HDFS",
	"YARN",
	"COS",
	"CHDFS",
}

func (ot ObjectType) String() string {
	if ot >= HiveService && ot <= Chdfs {
		return objectTypeName[ot]
	}
	return "unknown service type"
}
