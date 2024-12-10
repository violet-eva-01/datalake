// Package ranger @author: Violet-Eva @date  : 2024/11/25 @notes :
package ranger

import (
	"github.com/violet-eva-01/datalake/util"
	"strings"
	"time"
)

type Authorize struct {
	PolicyName        string   `json:"policy_name"`
	PermissionType    string   `json:"permission_type"`
	Permission        []string `json:"permission"`
	ObjectType        string   `json:"object_type"`
	ObjectName        string   `json:"object_name"`
	ObjectDBName      string   `json:"object_db_name"`
	ObjectTBLName     string   `json:"object_tbl_name"`
	ObjectColumnName  []string `json:"object_column"`
	ObjectRestriction []string `json:"object_restriction"`
	GranteeType       string   `json:"grantee_type"`
	Grantee           string   `json:"grantee"`
	IsEnable          bool     `json:"is_enable"`
	IsOverride        bool     `json:"is_override"`
	// ValiditySchedules
	// @Description: startTime~endTime~timeZone 2006-01-02 15:04:05~2006-01-03 15:04:05~Asia/Shanghai
	ValiditySchedules []string `json:"validity_schedules"`
	Status            bool     `json:"status"`
}

type object struct {
	ObjectType       string   `json:"object_type"`
	ObjectName       string   `json:"object_name"`
	ObjectDBName     string   `json:"object_db_name"`
	ObjectTBLName    string   `json:"object_tbl_name"`
	ObjectColumnName []string `json:"object_column_name"`
}

func getObjectType(policy PolicyBody) ObjectType {

	switch policy.ServiceType {
	case "hive":
		if len(policy.DataMaskPolicyItems) > 0 {
			return Masking
		} else if len(policy.RowFilterPolicyItems) > 0 {
			return RowFilter
		} else if len(policy.Resources.HiveService.Values) > 0 {
			return HiveService
		} else if len(policy.Resources.Url.Values) > 0 {
			return Url
		} else if len(policy.Resources.Udf.Values) > 0 {
			if len(policy.Resources.Database.Values) > 1 {
				return Udf
			} else {
				return GlobalUdf
			}
		} else if len(policy.Resources.Column.Values) > 0 {
			return Column
		} else if len(policy.Resources.Table.Values) > 0 {
			return Table
		} else {
			return Database
		}
	default:
		objectType := ObjectType(util.FindIndex(strings.ToUpper(policy.ServiceType), objectTypeName))
		return objectType
	}
}

func getObject(policy PolicyBody) (output []object) {

	objectType := getObjectType(policy)

	switch objectType {
	case HiveService:
		for _, hiveService := range policy.Resources.HiveService.Values {
			var tmpObject object
			if hiveService == "*" {
				hiveService = "ALL HIVE SERVICE"
			}
			tmpObject.ObjectName = hiveService
			tmpObject.ObjectType = HiveService.String()
			output = append(output, tmpObject)
		}
	case GlobalUdf:
		for _, gu := range policy.Resources.Global.Values {
			if gu == "*" {
				gu = "ALL GLOBAL UDF"
			}
			var tmpObject object
			tmpObject.ObjectName = gu
			tmpObject.ObjectType = GlobalUdf.String()
			output = append(output, tmpObject)
		}
	case Url:
		for _, url := range policy.Resources.Url.Values {
			if url == "*" {
				url = "ALL URL"
			}
			var tmpObject object
			tmpObject.ObjectName = url
			tmpObject.ObjectType = Url.String()
			output = append(output, tmpObject)
		}
	case Database:
		for _, db := range policy.Resources.Database.Values {
			if db == "*" {
				db = "ALL DATABASE"
			}
			var tmpObject object
			tmpObject.ObjectDBName = db
			tmpObject.ObjectType = Database.String()
			output = append(output, tmpObject)
		}
	case Hdfs:
		for _, path := range policy.Resources.Path.Values {
			if path == "*" {
				path = "ALL PATH"
			}
			var tmpObject object
			tmpObject.ObjectName = path
			tmpObject.ObjectType = Hdfs.String()
			output = append(output, tmpObject)
		}
	case Yarn:
		for _, query := range policy.Resources.Queue.Values {
			if query == "*" {
				query = "ALL QUEUE"
			}
			var tmpObject object
			tmpObject.ObjectName = query
			tmpObject.ObjectType = Yarn.String()
		}
	// 为*规则不生效，不做特殊处理
	case Masking, RowFilter:
		var tmpObject object
		tmpObject.ObjectDBName = policy.Resources.Database.Values[0]
		tmpObject.ObjectTBLName = policy.Resources.Table.Values[0]
		tmpObject.ObjectType = RowFilter.String()
		if objectType == Masking {
			tmpObject.ObjectColumnName = policy.Resources.Column.Values
			tmpObject.ObjectType = Masking.String()
		}
		output = append(output, tmpObject)
	case Chdfs:
		for _, mountPoint := range policy.Resources.MountPoint.Values {
			if mountPoint == "*" {
				mountPoint = "ALL MOUNT POINT"
			}
			for _, path := range policy.Resources.Path.Values {
				if path == "*" {
					path = "ALL PATH"
				}
				var tmpObject object
				tmpObject.ObjectName = mountPoint + " AND " + path
				tmpObject.ObjectType = Chdfs.String()
				output = append(output, tmpObject)
			}
		}
	case Cos:
		for _, bucket := range policy.Resources.Bucket.Values {
			if bucket == "*" {
				bucket = "ALL BUCKET"
			}
			for _, path := range policy.Resources.Path.Values {
				if path == "*" {
					path = "ALL PATH"
				}
				var tmpObject object
				tmpObject.ObjectName = bucket + " AND " + path
				tmpObject.ObjectType = Cos.String()
				output = append(output, tmpObject)
			}
		}
	case Table, Column:
		for _, database := range policy.Resources.Database.Values {
			if database == "*" {
				database = "ALL DATABASE"
			}
			for _, table := range policy.Resources.Table.Values {
				if table == "*" {
					table = "ALL TABLE"
				}
				var tmpObject object
				tmpObject.ObjectDBName = database
				tmpObject.ObjectTBLName = table
				tmpObject.ObjectType = Table.String()
				if objectType == Column {
					tmpObject.ObjectColumnName = policy.Resources.Column.Values
					tmpObject.ObjectType = Column.String()
				}

				output = append(output, tmpObject)
			}
		}
	default:
		panic("unhandled default case")
	}

	return
}

// rangerVSParse
// @Description:
// @param input 2006/1/2 15:04:05
// @return output
// @return err
func rangerVSParse(input string) (output string) {
	var (
		year, mount, day     string
		hour, minute, second string
	)
	timeArr := strings.Split(input, " ")
	splitYMD := strings.Split(timeArr[0], "/")
	year = splitYMD[0]
	if len(splitYMD[1]) != 2 {
		mount = "0" + splitYMD[1]
	} else {
		mount = splitYMD[1]
	}
	if len(splitYMD[2]) != 2 {
		day = "0" + splitYMD[2]
	} else {
		day = splitYMD[2]
	}

	splitHMS := strings.Split(timeArr[1], ":")
	if len(splitHMS[0]) != 2 {
		hour = "0" + hour
	} else {
		hour = splitHMS[0]
	}
	if len(splitHMS[1]) != 2 {
		minute = "0" + minute
	} else {
		minute = splitHMS[1]
	}
	if len(splitHMS[2]) != 2 {
		second = "0" + second
	} else {
		second = splitHMS[2]
	}

	output = year + "-" + mount + "-" + day + " " + hour + ":" + minute + ":" + second

	return
}

func getValiditySchedules(vss []ValiditySchedules) (output []string) {

	for _, vs := range vss {
		startTime := rangerVSParse(vs.StartTime)
		endTime := rangerVSParse(vs.EndTime)
		tmpStr := startTime + "~" + endTime + "~" + vs.TimeZone
		output = append(output, tmpStr)
	}

	return
}

func judgeTimeout(vss []string) (isTimeout bool, err error) {

	for _, vs := range vss {
		timeArr := strings.Split(vs, "~")
		var location *time.Location
		var parse time.Time
		location, err = time.LoadLocation(timeArr[2])
		if err != nil {
			return
		}
		parse, err = time.ParseInLocation("2006-01-02 15:04:05", timeArr[1], location)
		if err != nil {
			return
		}
		localTime := parse.Local()
		if time.Now().Local().After(localTime) {
			isTimeout = true
		} else {
			isTimeout = false
		}
	}
	return
}

func (a *Authorize) assignment(policy PolicyBody, oj object, permissions []string, permissionType string, grantee string, GranteeType string, vss []string, isTimeout bool, restrictions ...string) {
	a.PolicyName = policy.Name
	a.PermissionType = permissionType
	a.Permission = permissions
	a.ObjectType = oj.ObjectType
	a.ObjectName = oj.ObjectName
	a.ObjectDBName = strings.TrimSpace(oj.ObjectDBName)
	a.ObjectTBLName = strings.TrimSpace(oj.ObjectTBLName)
	a.ObjectColumnName = oj.ObjectColumnName
	a.ObjectRestriction = restrictions
	a.GranteeType = GranteeType
	a.Grantee = strings.TrimSpace(grantee)
	a.IsEnable = policy.IsEnabled
	a.IsOverride = policy.PolicyPriority != 0
	a.ValiditySchedules = vss
	if !a.IsEnable || isTimeout || (len(policy.AllowExceptions) > 0 && len(policy.PolicyItems) <= 0 && permissionType == "AllowException") || (len(policy.DenyExceptions) > 0 && len(policy.DenyPolicyItems) <= 0 && permissionType == "DenyException") {
		a.Status = false
	} else {
		a.Status = true
	}
}

func authorizeSliceAssignment(policy PolicyBody, ojs []object, users []string, roles []string, groups []string, permissions []string, permissionType string, vss []string, isTimeout bool, restrictions ...string) (output []Authorize) {

	for _, oj := range ojs {
		if len(users) > 1 || (len(users) == 1 && strings.TrimSpace(users[0]) != "") {
			for _, user := range users {
				var tmpAuth Authorize
				tmpAuth.assignment(policy, oj, permissions, permissionType, user, "USER", vss, isTimeout, restrictions...)
				output = append(output, tmpAuth)
			}
		}
		if len(groups) > 1 || (len(groups) == 1 && strings.TrimSpace(groups[0]) != "") {
			for _, group := range groups {
				var tmpAuth Authorize
				tmpAuth.assignment(policy, oj, permissions, permissionType, group, "GROUP", vss, isTimeout, restrictions...)
				output = append(output, tmpAuth)
			}
		}
		if len(roles) > 1 || (len(roles) == 1 && strings.TrimSpace(roles[0]) != "") {
			for _, role := range roles {
				var tmpAuth Authorize
				tmpAuth.assignment(policy, oj, permissions, permissionType, role, "ROLE", vss, isTimeout, restrictions...)
				output = append(output, tmpAuth)
			}
		}
	}

	return
}

func getPermissions(as []Accesses) (output []string) {
	for _, i := range as {
		output = append(output, i.Type)
	}
	return
}

func (pb *PolicyBody) policyBodyParse() ([]Authorize, error) {
	var (
		authorizes []Authorize
		vss        []string
		isTimeout  bool
	)

	objects := getObject(*pb)
	if len(pb.ValiditySchedules) > 0 {
		vss = getValiditySchedules(pb.ValiditySchedules)
		timeout, err := judgeTimeout(vss)
		if err != nil {
			return nil, err
		}
		isTimeout = timeout
	}

	if len(pb.RowFilterPolicyItems) > 0 {
		for _, rf := range pb.RowFilterPolicyItems {
			permissions := getPermissions(rf.Accesses)
			restriction := rf.RowFilterInfo.FilterExpr
			authorizeSlice := authorizeSliceAssignment(*pb, objects, rf.Users, rf.Roles, rf.Groups, permissions, "", vss, isTimeout, restriction)
			authorizes = append(authorizes, authorizeSlice...)
		}
	}

	if len(pb.DataMaskPolicyItems) > 0 {
		for _, dmp := range pb.DataMaskPolicyItems {
			permissions := getPermissions(dmp.Accesses)
			restriction := dmp.DataMaskInfo.DataMaskType
			authorizeSlice := authorizeSliceAssignment(*pb, objects, dmp.Users, dmp.Roles, dmp.Groups, permissions, "", vss, isTimeout, restriction)
			authorizes = append(authorizes, authorizeSlice...)
		}
	}

	if len(pb.PolicyItems) > 0 {
		permissionType := "PolicyItem"
		for _, pi := range pb.PolicyItems {
			permissions := getPermissions(pi.Accesses)
			authorizeSlice := authorizeSliceAssignment(*pb, objects, pi.Users, pi.Roles, pi.Groups, permissions, permissionType, vss, isTimeout)
			authorizes = append(authorizes, authorizeSlice...)
		}
	}

	if len(pb.DenyPolicyItems) > 0 {
		permissionType := "DenyPolicyItem"
		for _, dpi := range pb.DenyPolicyItems {
			permissions := getPermissions(dpi.Accesses)
			authorizeSlice := authorizeSliceAssignment(*pb, objects, dpi.Users, dpi.Roles, dpi.Groups, permissions, permissionType, vss, isTimeout)
			authorizes = append(authorizes, authorizeSlice...)
		}
	}

	if len(pb.AllowExceptions) > 0 {
		permissionType := "AllowException"
		for _, ae := range pb.AllowExceptions {
			permissions := getPermissions(ae.Accesses)
			authorizeSlice := authorizeSliceAssignment(*pb, objects, ae.Users, ae.Roles, ae.Groups, permissions, permissionType, vss, isTimeout)
			authorizes = append(authorizes, authorizeSlice...)
		}
	}

	if len(pb.DenyExceptions) > 0 {
		permissionType := "DenyException"
		for _, de := range pb.DenyExceptions {
			permissions := getPermissions(de.Accesses)
			authorizeSlice := authorizeSliceAssignment(*pb, objects, de.Users, de.Roles, de.Groups, permissions, permissionType, vss, isTimeout)
			authorizes = append(authorizes, authorizeSlice...)
		}
	}

	return authorizes, nil
}

func (r *Ranger) AccessParse(st ServiceType, filters ...func([]Authorize) []Authorize) ([]Authorize, error) {

	var (
		authorizes []Authorize
	)

	if r.ServicePolicyBodies[st.String()] == nil {
		gpErr := r.GetPolicy(st.String())
		if gpErr != nil {
			return nil, gpErr
		}
	}

	for _, policy := range r.ServicePolicyBodies[st.String()] {
		authorizeSlice, err := policy.policyBodyParse()
		if err != nil {
			return nil, err
		}
		authorizes = append(authorizes, authorizeSlice...)
	}

	for _, filter := range filters {
		authorizes = filter(authorizes)
	}

	return authorizes, nil
}
