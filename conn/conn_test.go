// Package conn @author: Violet-Eva @date  : 2024/12/20 @notes :
package conn

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/google/uuid"
	"reflect"
	"testing"
	"time"
)

type Water struct {
	Word  string          `json:"word" spark:"word_name"`
	Sale  float32         `json:"sale" spark:"sale_name"`
	Count int64           `json:"count"` // spark:"count_name"`
	Times arrow.Timestamp `json:"times"`
}

func TestDFToMap(t *testing.T) {
	sql, err := NewSparkSQL("127.0.0.1", 15002, 1000000)
	if err != nil {
		t.Fatal(err)
	}
	var ws []Water
	for i := 0; i < 10; i++ {
		var w Water
		w.Word = fmt.Sprintf("w%d", i)
		w.Sale = float32(i)
		w.Count = int64(i)
		w.Times = arrow.Timestamp(i)
		ws = append(ws, w)
	}
	frame, err := sql.CreateDataFrameFromStruct(context.Background(), ws, true)
	if err != nil {
		t.Fatal(err)
	}
	err = frame.Show(context.Background(), 100, false)
	if err != nil {
		t.Fatal(err)
	}
	collect, err := frame.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]interface{}
	for _, row := range collect {
		rec := make(map[string]interface{})
		for _, i := range row.FieldNames() {
			rec[i] = row.Value(i)
		}
		rows = append(rows, rec)
	}
}

func TestStructToDF(t *testing.T) {
	param := map[string]string{}
	param["user_id"] = "aldenDong"
	param["session_id"] = uuid.NewString()
	fmt.Println(param)
	sql, err := NewSparkSQL("127.0.0.1", 15002, 1000000, param)
	if err != nil {
		t.Fatal(err)
	}
	var ws []Water
	for i := 0; i < 10; i++ {
		var w Water
		w.Word = fmt.Sprintf("w%d", i)
		w.Sale = float32(i)
		w.Count = int64(i)
		ws = append(ws, w)
	}
	frame, err := sql.CreateDataFrameFromStruct(context.Background(), ws, true)
	if err != nil {
		t.Fatal(err)
	}
	err = frame.Show(context.Background(), 100, false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMapToDF(t *testing.T) {
	param := map[string]string{}
	param["token"] = "abckef"
	sql, err := NewSparkSQL("127.0.0.1", 15002, 1000000, param)
	if err != nil {
		t.Fatal(err)
	}
	var ws []map[string]interface{}
	for i := 0; i < 10; i++ {
		w := map[string]interface{}{}
		w["word"] = fmt.Sprintf("w%d", i)
		w["sale"] = float32(i)
		w["count"] = int64(i)
		ws = append(ws, w)
	}
	frame, err := sql.CreateDataFrameFromMap(context.Background(), Water{}, true, true, ws...)
	if err != nil {
		t.Fatal(err)
	}
	frame.Show(context.Background(), 100, false)
}

func TestConn(t *testing.T) {
	param := map[string]string{}
	param["token"] = "abckef"
	sql, err := NewSparkSQL("127.0.0.1", 15002, 1000000, param)
	if err != nil {
		t.Fatal(err)
	}
	//frame, err := sql.SS.Sql(context.Background(), "select 'apple' as word, cast(123.22 as decimal(38,18)) as count union all select 'orange' as word, cast(456.11 as decimal(38,18)) as count")
	frame, err := sql.Sql(context.Background(), "select 'apple' as word,123.11 as count union all select 'orange' as word, 456.22 as count")
	if err != nil {
		t.Fatal(err)
	}
	collect, err := frame.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var fields []types.StructField
	names := collect[0].FieldNames()
	for _, name := range names {
		fmt.Printf("name: [%s],value: [%v]\n", name, collect[0].Value(name))
		//dataType := scalar.MakeScalar(collect[0].Value(name)).DataType()
		dataType := CaseArrowType(collect[0].Value(name))
		fmt.Println(dataType)
		a := caseType(dataType)
		fmt.Println(a.TypeName())
		field := types.NewStructField(name, a)
		fmt.Println(field)
		fields = append(fields, field)
	}
	schema := types.StructOf(fields...)
	var rows [][]interface{}
	for index, row := range collect {
		var record []interface{}
		for _, name := range names {
			record = append(record, row.Value(name))
		}
		rows = append(rows, record)
		if (index+1)%2 == 0 {
			dataframe, err := sql.CreateDataFrame(context.Background(), rows, schema)
			if err != nil {
				t.Fatal(err)
			}
			dataframe.Show(context.Background(), 100, false)
		}
	}
}

// [ 1,  9] => Decimal32Type
// [10, 18] => Decimal64Type
// [19, 38] => Decimal128Type
// [39, 76] => Decimal256Type
func CaseArrowType(data interface{}) arrow.DataType {
	vf := reflect.ValueOf(data)

	switch vf.Interface().(type) {
	case int8:
		return arrow.PrimitiveTypes.Int8
	case int16:
		return arrow.PrimitiveTypes.Int16
	case int32:
		return arrow.PrimitiveTypes.Int32
	case int64:
		return arrow.PrimitiveTypes.Int64
	case bool:
		return arrow.FixedWidthTypes.Boolean
	case float32:
		return arrow.PrimitiveTypes.Float32
	case float64:
		return arrow.PrimitiveTypes.Float64
	case time.Time:
		return arrow.FixedWidthTypes.Date32
	case string:
		return arrow.BinaryTypes.String
	default:
		panic(fmt.Errorf("unknown type: %v", vf.Interface()))
	}
}

func MatchKind(data interface{}) string {
	vf := reflect.ValueOf(data)
	switch vf.Kind() {
	case reflect.Slice:
		return "slice"
	case reflect.Array:
		return "array"
	case reflect.Struct:
		return "struct"
	case reflect.Map:
		return "map"
	case reflect.Chan:
		return "chan"
	case reflect.Func:
		return "func"
	case reflect.Ptr:
		return "ptr"
	case reflect.Interface:
		return "interface"
	case reflect.Invalid:
		return "invalid"
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return "uint"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Complex64, reflect.Complex128:
		return "complex"
	default:
		return fmt.Sprintf("%v", vf.Interface())
	}
}

func caseType(dataType arrow.DataType) types.DataType {
	switch {
	case dataType == arrow.FixedWidthTypes.Boolean:
		return types.BOOLEAN
	case dataType == arrow.PrimitiveTypes.Int8:
		return types.BYTE
	case dataType == arrow.PrimitiveTypes.Int16:
		return types.SHORT
	case dataType == arrow.PrimitiveTypes.Int32:
		return types.INTEGER
	case dataType == arrow.PrimitiveTypes.Int64:
		return types.LONG
	case dataType == arrow.PrimitiveTypes.Float32:
		return types.FLOAT
	case dataType == arrow.PrimitiveTypes.Float64:
		return types.DOUBLE
	case dataType == arrow.BinaryTypes.String:
		return types.STRING
	case dataType.ID() == arrow.TIMESTAMP:
		return types.TIMESTAMP
	case dataType == arrow.FixedWidthTypes.Date32:
		return types.DATE
	default:
		panic("unhandled default case")
		return nil
	}
}
