package StarRocks

import (
	"fmt"
	"github.com/violet-eva-01/datalake/util"
	"regexp"
	"sort"
	"strings"
)

type ActionParse struct {
	Query             string
	Catalog           string
	DbName            string
	ParseTables       []Table
	SelectTableName   []string
	AlterTableName    []string
	InsertTableName   []string
	CreatTableName    []string
	DropTableName     []string
	DeleteTableName   []string
	UpdateTableName   []string
	TruncateTableName []string
	ErrorTables       []string
	withTablaName     []string
	fromTableName     []string
	joinTableName     []string
	extractTime       []string
	excludeTables     []string
	excludeSign       []string
}

type Table struct {
	Catalog   string
	DbName    string
	Action    string
	TableName string
	Index     int
}

type sqlParseRegexp struct {
	Reg *regexp.Regexp
	New string
}

func newRegexp(reg string, new string) *sqlParseRegexp {
	compile := regexp.MustCompile(reg)
	return &sqlParseRegexp{
		Reg: compile,
		New: new,
	}
}

func findAllStrings(str string, regArr ...*regexp.Regexp) (result []string) {
	for _, reg := range regArr {
		findAllString := reg.FindAllString(str, -1)
		for _, f := range findAllString {
			if len(f) > 0 {
				result = append(result, f)
			}
		}
	}
	return
}

func regexpReplaceAllStrings(strArr []string, regArr ...*sqlParseRegexp) (result []string) {

	for _, str := range strArr {
		for _, reg := range regArr {
			str = reg.Reg.ReplaceAllString(str, reg.New)
		}
		if len(str) > 0 {
			result = append(result, str)
		}
	}
	return
}

func NewSQLParse(query string, catalog string, dbName string, defaultCatalog string) *ActionParse {

	if len(strings.ReplaceAll(catalog, " ", "")) < 1 {
		if len(defaultCatalog) > 0 {
			catalog = defaultCatalog
		} else {
			catalog = "default_catalog"
		}
	}

	return &ActionParse{
		Query:   query,
		Catalog: catalog,
		DbName:  dbName,
	}
}

func (p *ActionParse) QueryClearAnnotation(isClean bool) {

	var (
		tmpStrArr   []string
		finalStrArr []string
	)

	replaceRegexp1 := regexp.MustCompile("(\\\\n|/\\*([^*]|\\*[^/])*\\*/)")
	tmpQuery := replaceRegexp1.ReplaceAllString(p.Query, "\n")

	replaceRegexp2 := newRegexp(`'((?:\\.|[^\\'])*)'`, " ")
	replaceRegexp3 := newRegexp(`"((?:\\.|[^\\"])*)"`, " ")
	replaceRegexp4 := newRegexp("--.*$", " ")

	if isClean {
		tmpStrArr = regexpReplaceAllStrings(strings.Split(tmpQuery, "\n"), replaceRegexp2, replaceRegexp3, replaceRegexp4)
	} else {
		tmpStrArr = regexpReplaceAllStrings(strings.Split(tmpQuery, "\n"), replaceRegexp4)
	}

	for _, str := range tmpStrArr {
		if len(strings.TrimSpace(str)) > 0 {
			finalStrArr = append(finalStrArr, str)
		}
	}

	if isClean {
		finalStrArr = regexpReplaceAllStrings([]string{strings.Join(finalStrArr, "\n")}, replaceRegexp2, replaceRegexp3)
	}

	p.Query = strings.Join(finalStrArr, "\n")
}

func (p *ActionParse) GetCatalogDB() {
	queryArr := strings.Split(p.Query, ";")
	if len(queryArr) < 2 {
		return
	}
	for _, query := range queryArr {
		p.getSet(query)
		p.getUse(query)
	}
}

func (p *ActionParse) getSet(str string) {

	parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)set\\s+catalog(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)\\s*")
	result := findAllStrings(str, parseFindRegexp)
	if len(result) <= 0 {
		return
	}
	parseReplaceRegexp1 := newRegexp("(?i)((^|\\s+|\\\\n)set\\s+catalog\\s+|\\s*)", "")
	parseReplaceRegexp2 := newRegexp("(?i)(^|\\s+|\\\\n)set\\s+catalog`", "`")
	tmpStrArr := regexpReplaceAllStrings(result, parseReplaceRegexp1, parseReplaceRegexp2)
	if len(tmpStrArr) <= 0 {
		return
	}
	p.Catalog = strings.ToLower(strings.ReplaceAll(tmpStrArr[0], "`", ""))

}

func (p *ActionParse) getUse(str string) {
	parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)use(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_\\p{L}]+|`[^`]+`))?\\s*")
	result := findAllStrings(str, parseFindRegexp)
	if len(result) <= 0 {
		return
	}
	parseReplaceRegexp1 := newRegexp("(?i)((^|\\s+|\\\\n)use\\s+|\\s*)", "")
	parseReplaceRegexp2 := newRegexp("(?i)(^|\\s+|\\\\n)use`", "`")
	tmpStrArr := regexpReplaceAllStrings(result, parseReplaceRegexp1, parseReplaceRegexp2)
	if len(tmpStrArr) <= 0 {
		return
	}
	catalogDB := strings.ReplaceAll(tmpStrArr[0], "`", "")
	strArr := strings.Split(strings.ToLower(catalogDB), ".")
	switch len(strArr) {
	case 1:
		p.DbName = strArr[0]
	case 2:
		p.Catalog = strArr[0]
		p.DbName = strArr[1]
	default:
		return
	}
}

// getTableNames
// @Description
// @param action
// @return error
func (p *ActionParse) getTableNames(action Action, isExists bool) {

	var (
		tableNames []string
	)

	switch action {
	case extractTimeAction:
		parseFindRegexp := regexp.MustCompile("(?i)extract\\s*\\([^)]+from(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_\\p{L}]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)extract\\s*\\([^)]+from\\s+", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	case fromAction:
		parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)from(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_\\p{L}]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp1 := newRegexp("(?i)((^|\\s+|\\\\n)from\\s+|\\s*)", "")
		parseReplaceRegexp2 := newRegexp("(?i)(^|\\s+|\\\\n)from`", "`")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp1, parseReplaceRegexp2)
	case withAction:
		parseFindRegexp := regexp.MustCompile("(?i)(with(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)(\\s*\\([^)]+\\))?\\s+as\\s*\\(|,\\s*([a-z0-9_\\p{L}]+|`[^`]+`)(\\s*\\([^)]+\\))?\\s+as\\s*\\()")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)(with\\s+|,\\s*|(\\s*\\([^)]+\\))?\\s+as\\s*\\()", "")
		parseReplaceRegexp2 := newRegexp("(?i)with`", "`")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp, parseReplaceRegexp2)
	case InsertAction:
		parseFindRegexp := regexp.MustCompile("(?i)insert\\s+(into|overwrite)(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)insert\\s+(into|overwrite)\\s+", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	case DropAction:
		parseFindRegexp := regexp.MustCompile("(?i)drop\\s+(temporary\\s+)?(table|view|materialized\\s+view)+(\\s+if\\s+exists)?(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)drop\\s+(temporary\\s+)?(table|view|materialized\\s+view)+(\\s+if\\s+exists)?\\s*", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	case CreateAction:
		parseFindRegexp := regexp.MustCompile("(?i)create\\s+(table|view|materialized\\s+view)+(\\s+if\\s+not\\s+exists)?(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)create\\s+(table|view|materialized\\s+view)+(\\s+if\\s+not\\s+exists)?\\s*", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	case joinAction:
		tableName := "(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_\\p{L}]+|`[^`]+`))*(\\s*as)?(\\s*[a-z0-9]+)?"
		parseFindRegexp1 := regexp.MustCompile(fmt.Sprintf("(?i)from%s(\\s*,\\s*%s)*", tableName, tableName))
		parseReplaceRegexp1 := newRegexp(fmt.Sprintf("(?i)from%s", tableName), "")
		parseReplaceRegexp2 := newRegexp("\\s*,\\s*", ",")
		parseReplaceRegexp3 := newRegexp("\\s*\\.\\s*", ".")
		parseFindRegexp2 := regexp.MustCompile("(?i)([a-z0-9_\\p{L}]+|`[^`]+`)(\\s*\\.\\s*([a-z0-9_\\p{L}]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp1)
		tmpTables := regexpReplaceAllStrings(result, parseReplaceRegexp1, parseReplaceRegexp2, parseReplaceRegexp3)
		for _, tmpTable := range tmpTables {
			for _, tmpTBL := range strings.Split(tmpTable, ",") {
				if strings.ReplaceAll(tmpTBL, " ", "") == "" {
					continue
				}
				tmpTableNames := findAllStrings(tmpTBL, parseFindRegexp2)
				if len(tmpTableNames) > 0 {
					tableNames = append(tableNames, tmpTableNames[0])
				}
			}
		}
		parseFindRegexp3 := regexp.MustCompile("(?i)(^|\\s+|\\\\n)join(\\s+[a-z0-9_\\p{L}]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_\\p{L}]+|`[^`]+`))*")
		result1 := findAllStrings(p.Query, parseFindRegexp3)
		parseReplaceRegexp5 := newRegexp("(?i)(^|\\s+|\\\\n)join\\s+", "")
		parseReplaceRegexp6 := newRegexp("(?i)(^|\\s+|\\\\n)join`", "`")
		tmpTableNames := regexpReplaceAllStrings(result1, parseReplaceRegexp5, parseReplaceRegexp6, parseReplaceRegexp3)
		tableNames = append(tableNames, tmpTableNames...)
	case SelectAction:
	case AlterAction:
		parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)alter\\s+(table|view|materialized\\s+view)+(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)(^|\\s+|\\\\n)alter\\s+(table|view|materialized\\s+view)+\\s*", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	case DeleteAction:
		parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)delete\\s+from(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)(^|\\s+|\\\\n)delete\\s+from\\s*", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	case UpdateAction:
		parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)update(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp1 := newRegexp("(?i)((^|\\s+|\\\\n)update\\s+|\\s*)", "")
		parseReplaceRegexp2 := newRegexp("(?i)(^|\\s+|\\\\n)update`", "`")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp1, parseReplaceRegexp2)
	case TruncateAction:
		parseFindRegexp := regexp.MustCompile("(?i)(^|\\s+|\\\\n)truncate\\s+table(\\s+[a-z0-9_]+|\\s*`[^`]+`)(\\s*\\.\\s*([a-z0-9_]+|`[^`]+`))*")
		result := findAllStrings(p.Query, parseFindRegexp)
		parseReplaceRegexp := newRegexp("(?i)(^|\\s+|\\\\n)truncate\\s+table\\s*", "")
		tableNames = regexpReplaceAllStrings(result, parseReplaceRegexp)
	default:
		return
	}

	p.assignment(action, tableNames, isExists)

	return
}

func (p *ActionParse) AddExcludeTables(excludeTables ...string) {
	p.excludeTables = util.RemoveRepeatElementAndToLower(append(p.excludeTables, addSpecialCharacters(excludeTables)...))
	sort.Strings(p.excludeTables)
}

func (p *ActionParse) InitExcludeTables(excludeTables ...string) {
	p.excludeTables = []string{"`dual`", "`unnest`", "`files`", "`generate_series`"}
	if len(excludeTables) > 0 {
		p.AddExcludeTables(excludeTables...)
	} else {
		sort.Strings(p.excludeTables)
	}
}

func (p *ActionParse) AddExcludeSign(excludeSign ...string) {
	p.excludeSign = util.RemoveRepeatElementAndToLower(append(p.excludeTables, addSpecialCharacters(excludeSign)...))
	sort.Strings(p.excludeSign)
}

func (p *ActionParse) InitExcludeSign(excludeSign ...string) {
	p.excludeSign = []string{"#tableau_"}
	if len(excludeSign) > 0 {
		p.AddExcludeSign(excludeSign...)
	} else {
		sort.Strings(p.excludeSign)
	}
}

func (p *ActionParse) GetSelectTables(isExists bool) {
	p.getTableNames(extractTimeAction, isExists)
	p.getTableNames(withAction, isExists)
	p.getTableNames(DeleteAction, isExists)
	p.getTableNames(fromAction, isExists)
	p.getTableNames(joinAction, isExists)
	p.getTableNames(SelectAction, isExists)
}

func (p *ActionParse) GetCreateTables(isExists bool) {
	p.getTableNames(CreateAction, isExists)
}

func (p *ActionParse) GetDropTables(isExists bool) {
	p.getTableNames(DropAction, isExists)
}

func (p *ActionParse) GetInsertTables(isExists bool) {
	p.getTableNames(InsertAction, isExists)
}

func (p *ActionParse) GetUpdateTables(isExists bool) {
	p.getTableNames(UpdateAction, isExists)
}

func (p *ActionParse) GetDeleteTables(isExists bool) {
	p.getTableNames(DeleteAction, isExists)
}

func (p *ActionParse) GetTruncateTables(isExists bool) {
	p.getTableNames(TruncateAction, isExists)
}

func (p *ActionParse) GetAlterTables(isExists bool) {
	p.getTableNames(AlterAction, isExists)
}

func (p *ActionParse) InitAllUseTable() {
	p.QueryClearAnnotation(true)
	p.GetCatalogDB()
	p.GetSelectTables(false)
	p.GetAlterTables(false)
	p.GetCreateTables(false)
	p.GetDropTables(true)
	p.GetInsertTables(false)
	p.GetUpdateTables(false)
	p.GetTruncateTables(false)
}

func (p *ActionParse) DebugGetSelectTables() {
	fmt.Println("clean 后的query")
	fmt.Println(p.Query)
	fmt.Println("查询表名")
	for _, i := range p.fromTableName {
		fmt.Println("fromTableName : ", i)
	}
	for _, i := range p.joinTableName {
		fmt.Println("joinTableName : ", i)
	}
	fmt.Println("除外表名")
	for _, i := range p.extractTime {
		fmt.Println("extractTime : ", i)
	}
	for _, i := range p.withTablaName {
		fmt.Println("withTablaName : ", i)
	}
	for _, i := range p.DeleteTableName {
		fmt.Println("DeleteTableName : ", i)
	}
	fmt.Println("除外常量表名")
	for _, i := range p.excludeTables {
		fmt.Println("excludeTables : ", i)
	}
	fmt.Println("除外常量标志")
	for _, i := range p.excludeSign {
		fmt.Println("excludeSign : ", i)
	}
	fmt.Println("最终查询表名")
	for _, i := range p.SelectTableName {
		fmt.Println("SelectTableName : ", i)
	}
}
