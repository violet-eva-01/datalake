package StarRocks

import (
	"fmt"
	"regexp"
	"strings"
)

type AuthorizeParse struct {
	UserIdentity string
	Catalog      string
	// AS
	// @Description: Authorization Statement
	AS        string
	as        string
	Authorize Authorize
}

type Authorize struct {
	Catalog string `gorm:"column:catalog" json:"catalog"`
	//PermissionsType string
	Permissions   []string `gorm:"column:permissions" json:"permissions"`
	ObjectType    string   `gorm:"column:object_type" json:"object_type"`
	ObjectName    string   `gorm:"column:object_name" json:"object_name"`
	ObjectDBName  string   `gorm:"column:object_db_name" json:"object_db_name"`
	ObjectTBLName string   `gorm:"column:object_tbl_name" json:"object_tbl_name"`
	GranteeType   string   `gorm:"column:grantee_type" json:"grantee_type"`
	GranteeName   string   `gorm:"column:grantee_name" json:"grantee_name"`
	IP            string   `gorm:"column:ip" json:"ip"`
	GrantSQL      string   `gorm:"column:grant_sql" json:"grant_sql"`
}

func NewGrantParse(userIdentity string, catalog string, as string) *AuthorizeParse {
	compile := regexp.MustCompile("(?i)^GRANT")
	return &AuthorizeParse{
		UserIdentity: userIdentity,
		Catalog:      catalog,
		AS:           as,
		as:           compile.ReplaceAllString(as, ""),
		Authorize:    Authorize{Catalog: catalog, GrantSQL: as},
	}
}

func (ap *AuthorizeParse) Parse() {
	ap.getGrantee()
	ap.getObject()
	ap.getPermissions()
}

func (ap *AuthorizeParse) getGrantee() {

	compile := regexp.MustCompile("(?i)\\s+TO\\s+.*$")
	ap.as = compile.ReplaceAllString(ap.as, "")

	split := strings.Split(strings.TrimSpace(strings.ReplaceAll(ap.UserIdentity, "'", "")), "@")
	switch len(split) {
	case 1:
		ap.Authorize.GranteeName = split[0]
		ap.Authorize.GranteeType = "ROLE"
	case 2:
		ap.Authorize.GranteeName = split[0]
		ap.Authorize.IP = split[1]
		ap.Authorize.GranteeType = "USER"
	}
}

func (ap *AuthorizeParse) getObject() {
	objectCompile := regexp.MustCompile("(?i)\\s+ON\\s+.*$")
	objectStrList := objectCompile.FindAllString(ap.as, -1)
	ap.as = objectCompile.ReplaceAllString(ap.as, "")
	if len(objectStrList) == 0 {
		ap.Authorize.ObjectType = "ROLE"
		return
	}
	objectStr := objectStrList[0]
	for sptIndex, sptName := range permissionTypeNames {
		var (
			objectTypeRegexp string
			policies         string
			policiesRegexp   string
		)

		spt := PermissionType(sptIndex)

		switch spt {
		case MaskingPolicyType, RowAccessPolicyType:
			policiesCompile := regexp.MustCompile("Y$")
			policiesRegexp = policiesCompile.ReplaceAllString(spt.RegexpString(), "IES")
			policies = policiesCompile.ReplaceAllString(spt.String(), "IES")
			objectTypeRegexp = fmt.Sprintf("(?i)(ON\\s+%s|ON\\s+ALL\\s+%s)", spt.RegexpString(), policiesRegexp)
		default:
			objectTypeRegexp = fmt.Sprintf("(?i)(ON\\s+%s|ON\\s+ALL\\s+%sS)", spt.RegexpString(), spt.RegexpString())
		}

		objectTypeCompile := regexp.MustCompile(objectTypeRegexp)

		if objectTypeCompile.MatchString(objectStr) {
			ap.Authorize.ObjectType = sptName
			switch spt {
			case SystemType:
				return
			case TableType, ViewType, MaterializedViewType, FunctionType:
				objectNameRegexpStr := fmt.Sprintf("(?i)ON\\s+ALL\\s+%sS", spt.RegexpString())
				objectNameCompile := regexp.MustCompile(objectNameRegexpStr)
				if objectNameCompile.MatchString(objectStr) {
					ap.Authorize.ObjectTBLName = fmt.Sprintf("ALL %sS", sptName)
					objectDBNameStr := objectNameCompile.ReplaceAllString(objectStr, "")
					if regexp.MustCompile("(?i)ALL\\s+DATABASES").MatchString(objectDBNameStr) {
						ap.Authorize.ObjectDBName = "ALL DATABASES"
					} else {
						objectDBCompile := regexp.MustCompile("(?i)(IN\\s+DATABASE|')")
						ap.Authorize.ObjectDBName = strings.TrimSpace(objectDBCompile.ReplaceAllString(objectDBNameStr, ""))
					}
				} else {
					objectDBTBLRegexpStr := fmt.Sprintf("(?i)(ON\\s+%s|')", spt.RegexpString())
					objectDBTBLStr := strings.Split(strings.TrimSpace(regexp.MustCompile(objectDBTBLRegexpStr).ReplaceAllString(objectStr, "")), ".")
					ap.Authorize.ObjectDBName = objectDBTBLStr[0]
					ap.Authorize.ObjectTBLName = objectDBTBLStr[1]
				}
			case MaskingPolicyType, RowAccessPolicyType:
				objectNameRegexpStr := fmt.Sprintf("(?i)ON\\s+ALL\\s+%s", policiesRegexp)
				objectNameCompile := regexp.MustCompile(objectNameRegexpStr)
				if objectNameCompile.MatchString(objectStr) {
					ap.Authorize.ObjectName = fmt.Sprintf("ALL %s", policies)
					objectDBNameStr := objectNameCompile.ReplaceAllString(objectStr, "")
					if regexp.MustCompile("(?i)ALL\\s+DATABASES").MatchString(objectDBNameStr) {
						ap.Authorize.ObjectDBName = "ALL DATABASES"
					} else {
						objectDBCompile := regexp.MustCompile("(?i)(IN\\s+DATABASE|')")
						ap.Authorize.ObjectDBName = strings.TrimSpace(objectDBCompile.ReplaceAllString(objectDBNameStr, ""))
					}
				} else {
					objectDBNameCompile := regexp.MustCompile("(?i)\\s+IN\\s.*$")
					objectDBNameStr := objectDBNameCompile.FindAllString(objectStr, -1)[0]
					if regexp.MustCompile("(?i)ALL\\s+DATABASES").MatchString(objectDBNameStr) {
						ap.Authorize.ObjectDBName = "ALL DATABASES"
					} else {
						objectDBCompile := regexp.MustCompile("(?i)(IN\\s+DATABASE|')")
						ap.Authorize.ObjectDBName = strings.TrimSpace(objectDBCompile.ReplaceAllString(objectDBNameStr, ""))
					}
					objectNameStr := objectDBNameCompile.ReplaceAllString(objectStr, "")
					objectNameCompile = regexp.MustCompile(fmt.Sprintf("(?i)(ON\\s+%s|')", spt.RegexpString()))
					ap.Authorize.ObjectName = strings.TrimSpace(objectNameCompile.ReplaceAllString(objectNameStr, ""))
				}
			case DatabaseType:
				if regexp.MustCompile(fmt.Sprintf("(?i)ON\\s+ALL\\s+%sS", spt.RegexpString())).MatchString(objectStr) {
					ap.Authorize.ObjectDBName = fmt.Sprintf("ALL %sS", sptName)
				} else {
					objectDBNameCompile := regexp.MustCompile(fmt.Sprintf("(?i)(ON\\s+%s|')", spt.RegexpString()))
					ap.Authorize.ObjectDBName = strings.TrimSpace(objectDBNameCompile.ReplaceAllString(objectStr, ""))
				}
			default:
				if regexp.MustCompile(fmt.Sprintf("(?i)ON\\s+ALL\\s+%sS", spt.RegexpString())).MatchString(objectStr) {
					ap.Authorize.ObjectName = fmt.Sprintf("ALL %sS", sptName)
				} else {
					objectNameCompile := regexp.MustCompile(fmt.Sprintf("(?i)(ON\\s+%s|')", spt.RegexpString()))
					ap.Authorize.ObjectName = strings.TrimSpace(objectNameCompile.ReplaceAllString(objectStr, ""))
				}
			}
			return
		}
	}
}

func (ap *AuthorizeParse) getPermissions() {
	switch ap.Authorize.ObjectType {
	case "ROLE":
		permissionList := strings.Split(strings.ReplaceAll(ap.as, "'", ""), ",")
		for _, i := range permissionList {
			ap.Authorize.Permissions = append(ap.Authorize.Permissions, strings.TrimSpace(i))
		}
	default:
		for spIndex, spName := range starRocksPermissionNames {
			sp := Permission(spIndex)
			permissionRegexpStr := fmt.Sprintf("(?i)\\s+%s(,)?", sp.RegexpString())
			permissionCompile := regexp.MustCompile(permissionRegexpStr)
			if permissionCompile.MatchString(ap.as) {
				ap.Authorize.Permissions = append(ap.Authorize.Permissions, spName)
				ap.as = permissionCompile.ReplaceAllString(ap.as, " ")
			}
		}
	}
}
