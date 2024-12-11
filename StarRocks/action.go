package StarRocks

import (
	"github.com/violet-eva-01/datalake/util"
	"regexp"
	"sort"
	"strings"
)

type Action int

const (
	extractTimeAction Action = 0 + iota
	withAction
	fromAction
	joinAction
	SelectAction
	AlterAction
	InsertAction
	CreateAction
	DropAction
	UpdateAction
	DeleteAction
	TruncateAction
)

var actionNames = []string{
	"EXTRACT TIME",
	"WITH",
	"FROM",
	"JOIN",
	"SELECT",
	"ALTER",
	"INSERT",
	"CREATE",
	"DROP",
	"UPDATE",
	"DELETE",
	"TRUNCATE",
}

func ParseActionName(str string) Action {

	index := util.FindIndex(strings.ToUpper(str), actionNames)
	if index == -1 {
		return -1
	} else {
		return Action(index)
	}

}

func (a Action) String() string {

	if a >= extractTimeAction && a <= TruncateAction {
		return actionNames[a]
	}

	return "nil"
}

func (p *ActionParse) assignment(code Action, tbl []string, isExists bool) {

	tbl = addSpecialCharacters(util.RemoveRepeatElementAndToLower(tbl))

	if isExists {
		tbl = util.RemoveMatchElement(tbl, p.excludeSign)
	}

	switch code {
	case extractTimeAction:
		p.extractTime = tbl
	case withAction:
		p.withTablaName = tbl
	case fromAction:
		p.fromTableName = tbl
	case joinAction:
		p.joinTableName = tbl
	case SelectAction:
		otherArr := append(p.extractTime, p.excludeTables...)
		otherArr = append(otherArr, p.withTablaName...)
		otherArr = append(otherArr, p.DeleteTableName...)
		sort.Strings(otherArr)
		tbl = util.RemoveCoincideElement(append(p.fromTableName, p.joinTableName...), otherArr, false)
		p.SelectTableName = tbl
	case InsertAction:
		p.InsertTableName = tbl
	case CreateAction:
		p.CreatTableName = tbl
	case DropAction:
		p.DropTableName = tbl
	case AlterAction:
		p.AlterTableName = tbl
	case DeleteAction:
		p.DeleteTableName = tbl
	case UpdateAction:
		p.UpdateTableName = tbl
	case TruncateAction:
		p.TruncateTableName = tbl
	default:
		return
	}

	p.assign(code, tbl)
}

func addSpecialCharacters(list []string) (result []string) {
	for _, v := range list {
		compile, _ := regexp.Compile("(`[^`]+`|[a-z0-9_]+)")
		allString := compile.FindAllString(v, -1)
		var tmpArr []string
		for _, str := range allString {
			if !strings.Contains(str, "`") {
				str = "`" + str + "`"
			}
			tmpArr = append(tmpArr, str)
		}
		v = strings.Join(tmpArr, ".")
		result = append(result, v)
	}
	return
}

func (p *ActionParse) assign(code Action, tbl []string) {
	switch code {
	case CreateAction, DropAction, InsertAction, AlterAction, DeleteAction, UpdateAction, TruncateAction, SelectAction:
	default:
		return
	}
	i := 0
	for _, table := range tbl {
		var t Table
		compile, _ := regexp.Compile("(`[^`]+`|[a-z0-9_]+)")
		allString := compile.FindAllString(table, -1)
		length := len(allString)
		switch length {
		case 1:
			if p.DbName == "" {
				p.ErrorTables = append(p.ErrorTables, table)
				continue
			}
			t.TableName = strings.ReplaceAll(allString[0], "`", "")
			t.DbName = strings.ReplaceAll(p.DbName, "`", "")
			t.Catalog = strings.ReplaceAll(p.Catalog, "`", "")
		case 2:
			t.TableName = strings.ReplaceAll(allString[1], "`", "")
			t.DbName = strings.ReplaceAll(allString[0], "`", "")
			t.Catalog = strings.ReplaceAll(p.Catalog, "`", "")
		case 3:
			t.TableName = strings.ReplaceAll(allString[2], "`", "")
			t.DbName = strings.ReplaceAll(allString[1], "`", "")
			t.Catalog = strings.ReplaceAll(allString[0], "`", "")
		default:
			p.ErrorTables = append(p.ErrorTables, table)
			continue
		}
		t.Action = code.String()
		t.Index = i
		p.ParseTables = append(p.ParseTables, t)
		i += 1
	}
}
