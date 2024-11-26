// Package ranger @author: Violet-Eva @date  : 2024/11/22 @notes :
package ranger

// Database
// @Description: database resource
type Database struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Table
// @Description: table resource
type Table struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Column
// @Description: column resource
type Column struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Global
// @Description: global resource
type Global struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// HiveService
// @Description: hiveService resource
type HiveService struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// UDF
// @Description: udf resource
type UDF struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// URL
// @Description: url resource
type URL struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Bucket
// @Description: tencent cos bucket resource
type Bucket struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Path
// @Description: cos / hdfs path resource
type Path struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// Queue
// @Description: yarn queue resource
type Queue struct {
	Values      []string `json:"values"`
	IsExcludes  bool     `json:"isExcludes"`
	IsRecursive bool     `json:"isRecursive"`
}

// KeyName
// @Description: kms keyName resource
type KeyName struct {
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
	PolicyPriority int    `json:"policyPriority"`
	Description    string `json:"description"`
	IsAuditEnabled bool   `json:"isAuditEnabled"`
	Resources      struct {
		// hive service 相关
		Database    Database    `json:"database,omitempty"`
		Table       Table       `json:"table,omitempty"`
		Column      Column      `json:"column,omitempty"`
		Global      Global      `json:"global,omitempty"`
		HiveService HiveService `json:"hiveservice,omitempty"`
		Udf         UDF         `json:"udf,omitempty"`
		Url         URL         `json:"url,omitempty"`
		// cos & hdfs service 相关
		Bucket Bucket `json:"bucket,omitempty"`
		Path   Path   `json:"path,omitempty"`
		// yarn service 相关
		Queue Queue `json:"queue,omitempty"`
		// kms service 相关
		KeyName KeyName `json:"keyname,omitempty"`
	} `json:"resources"`
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

type ServiceType int

const (
	Hive ServiceType = iota
	Hdfs
	Cos
	Yarn
	kms
)

var serviceTypeName = []string{
	"hive",
	"hdfs",
	"cos",
	"yarn",
	"kms",
}

func (st ServiceType) String() string {
	if st >= Hive && st <= kms {
		return serviceTypeName[st]
	}
	return "unknown service type"
}

func inServiceType(st int) bool {
	if len(serviceTypeName)-1 < st {
		return false
	} else {
		return true
	}
}

type ServiceTypeId struct {
	ServiceType   ServiceType `json:"serviceType"`
	ServiceTypeId int         `json:"serviceTypeId"`
}
