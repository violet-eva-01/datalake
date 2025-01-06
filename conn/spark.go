// Package conn @author: Violet-Eva @date  : 2024/12/26 @notes :
package conn

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/violet-eva-01/datalake/util"
	"reflect"
	"strings"
	"time"
)

type SparkSQL struct {
	SS      sql.SparkSession
	Timeout time.Duration
}

func NewSparkSQL(remote string, timeout time.Duration) (*SparkSQL, error) {
	ctx := context.Background()
	ss, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		return nil, err
	}
	return &SparkSQL{
		SS:      ss,
		Timeout: timeout,
	}, nil
}

// StructToStructType
// @Description: isTag is false , get struct elem name assign to structField name. isTag is true  , get json tag name assign to structField name.
// @param v
// @param isTag
// @return *types.StructType
// @return error
func StructToStructType(v interface{}, isRename bool) (*types.StructType, error) {
	var (
		fields    []types.StructField
		sparkTags map[string]string
	)
	vf := reflect.ValueOf(v)
	tf := reflect.TypeOf(v)
	if tf.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %T", v)
	}
	if isRename {
		sparkTags = util.ConvStructSparkTags(v, false)
	}
	for i := 0; i < vf.NumField(); i++ {
		var filed types.StructField
		if isRename {
			filed.Name = sparkTags[tf.Field(i).Name]
			if filed.Name == "" {
				filed.Name = tf.Field(i).Name
			}
		} else {
			filed.Name = tf.Field(i).Name
		}
		filed.Metadata = nil
		switch vt := vf.Field(i).Interface().(type) {
		case bool:
			filed.DataType = types.BOOLEAN
		case int8:
			filed.DataType = types.BYTE
		case int16:
			filed.DataType = types.SHORT
		case int32, int:
			filed.DataType = types.INTEGER
		case int64:
			filed.DataType = types.LONG
		case float32:
			filed.DataType = types.FLOAT
		case float64:
			filed.DataType = types.DOUBLE
		case string:
			filed.DataType = types.STRING
		case time.Time:
			filed.DataType = types.DATE
		case arrow.Timestamp:
			filed.DataType = types.TIMESTAMP
		default:
			panic(fmt.Errorf("unsupported data type: %s", vt))
		}
		fields = append(fields, filed)
	}
	return &types.StructType{
		Fields: fields,
	}, nil
}

// SAToTSA
// @Description: any slice  -> any 2D slicing
// @param structType
// @param data
// @return [][]interface{}
func SAToTSA(structType *types.StructType, data ...any) [][]interface{} {
	length := len(structType.Fields)
	var rows [][]interface{}
	for _, row := range data {
		var record []interface{}
		vf := reflect.ValueOf(row)
		for i := 0; i < length; i++ {
			rec := vf.Field(i).Interface()
			record = append(record, rec)
		}
		rows = append(rows, record)
	}
	return rows
}

// anyToSliceAny
// @Description: any -> any slice
// @param data
// @return []interface{}
func anyToSliceAny(data any) []interface{} {
	vf := reflect.ValueOf(data)
	if vf.Kind() != reflect.Slice && vf.Kind() == reflect.Struct {
		return []interface{}{data}
	}
	rows := make([]any, vf.Len())
	for i := 0; i < vf.Len(); i++ {
		rows[i] = vf.Index(i).Interface()
	}
	return rows
}

// CreateDataFrameFromStruct
// @Description:
// @param ctx
// @param data
// @param isRename == true ,Rename the dataframe based on the spark tag , Insufficient tags are supplemented by elem name
// @return sql.DataFrame
// @return error
func (s *SparkSQL) CreateDataFrameFromStruct(ctx context.Context, data any, isRename bool) (sql.DataFrame, error) {
	rows := anyToSliceAny(data)
	if len(rows) == 0 {
		return nil, fmt.Errorf("no data")
	}
	structType, err := StructToStructType(rows[0], isRename)
	if err != nil {
		return nil, err
	}
	sliceAny := SAToTSA(structType, rows...)
	return s.CreateDataFrame(ctx, sliceAny, structType)
}

func convStructDoubleTags(data any, tagName1, tagName2 string, splitKey ...[2]string) map[string]string {
	valueOf := reflect.ValueOf(data)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return nil
	}
	output := make(map[string]string, valueOf.NumField())
	if len(splitKey) > 0 {
		for i := 0; i < valueOf.NumField(); i++ {
			var (
				tag1Name string
				tag2Name string
			)
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tag1Value := tag.Get(tagName1)
			if tag1Value != "" {
				splitValue := strings.Split(tag1Value, ",")
				for _, Value := range splitValue {
					if strings.HasPrefix(Value, splitKey[0][0]) {
						tag1Name = strings.TrimPrefix(Value, splitKey[0][0])
					}
				}
			}
			tag2Value := tag.Get(tagName2)
			if tag2Value != "" {
				splitValue := strings.Split(tag1Value, ",")
				for _, Value := range splitValue {
					if strings.HasPrefix(Value, splitKey[0][0]) {
						tag2Name = strings.TrimPrefix(Value, splitKey[0][0])
					}
				}
			}
			output[tag1Name] = tag2Name
		}
	} else {
		for i := 0; i < valueOf.NumField(); i++ {
			field := valueOf.Type().Field(i)
			tag := field.Tag
			tag1Value := tag.Get(tagName1)
			if tag1Value == "" {
				tag1Value = field.Name
			}
			tag2Value := tag.Get(tagName2)
			output[tag1Value] = tag2Value
		}
	}
	return output
}

func mapToSliceAny(structType *types.StructType, v interface{}, isTag bool, isRename bool, data ...map[string]interface{}) ([][]interface{}, error) {
	var (
		mappingTags map[string]string
	)
	length := len(structType.Fields)
	if isTag && isRename {
		mappingTags = convStructDoubleTags(v, "spark", "json")
	} else if isRename {
		mappingTags = util.ConvStructSparkTags(v, false)
		mappingTags = util.MapTurnOver(mappingTags)
	} else if isTag {
		mappingTags = util.ConvStructJsonTags(v, false)
	}
	fmt.Println(mappingTags)
	var rows [][]interface{}
	for _, row := range data {
		fmt.Println(row)
		var record []interface{}
		for i := 0; i < length; i++ {
			var rec interface{}
			if len(mappingTags) > 0 {
				fmt.Println(structType.Fields[i].Name)
				fmt.Println(mappingTags[structType.Fields[i].Name])
				rec = row[mappingTags[structType.Fields[i].Name]]
				if rec == nil && !isTag {
					rec = row[structType.Fields[i].Name]
				}
				fmt.Println(rec)
			} else {
				rec = row[structType.Fields[i].Name]
			}
			record = append(record, rec)
		}
		rows = append(rows, record)
	}
	return rows, nil
}

// CreateDataFrameFromMap
// @Description: []map[string]interface{} -> sql.DataFrame , map
// @param ctx
// @param v
// @param isTag true ,Assign values to dataframes based on JSON tags. false, Assign values to dataframes based on elem name. This field is because it is not possible to ignore the case match to the value, and the field is added.
// @param isRename  true ,Rename the dataframe based on the spark tag , Insufficient tags are supplemented by elem name
// @param data
// @return sql.DataFrame
// @return error
func (s *SparkSQL) CreateDataFrameFromMap(ctx context.Context, v interface{}, isTag bool, isRename bool, data ...map[string]interface{}) (sql.DataFrame, error) {
	structType, err := StructToStructType(v, isRename)
	if err != nil {
		return nil, err
	}
	sliceAny, err := mapToSliceAny(structType, v, isTag, isRename, data...)
	if err != nil {
		return nil, err
	}
	return s.CreateDataFrame(ctx, sliceAny, structType)
}

func (s *SparkSQL) CreateDataFrame(ctx context.Context, data [][]any, schema *types.StructType) (sql.DataFrame, error) {
	pool := memory.NewGoAllocator()
	// Convert the data into an Arrow Table
	arrowSchema := arrow.NewSchema(schema.ToArrowType().Fields(), nil)
	rb := array.NewRecordBuilder(pool, arrowSchema)
	defer rb.Release()
	// Iterate over all fields and add the values:
	for _, row := range data {
		for i, field := range schema.Fields {
			if row[i] == nil {
				rb.Field(i).AppendNull()
				continue
			}
			switch field.DataType {
			case types.BOOLEAN:
				rb.Field(i).(*array.BooleanBuilder).Append(row[i].(bool))
			case types.BYTE:
				rb.Field(i).(*array.Int8Builder).Append(row[i].(int8))
			case types.SHORT:
				rb.Field(i).(*array.Int16Builder).Append(row[i].(int16))
			case types.INTEGER:
				rb.Field(i).(*array.Int32Builder).Append(row[i].(int32))
			case types.LONG:
				rb.Field(i).(*array.Int64Builder).Append(row[i].(int64))
			case types.FLOAT:
				rb.Field(i).(*array.Float32Builder).Append(row[i].(float32))
			case types.DOUBLE:
				rb.Field(i).(*array.Float64Builder).Append(row[i].(float64))
			case types.STRING:
				rb.Field(i).(*array.StringBuilder).Append(row[i].(string))
			case types.DATE:
				rb.Field(i).(*array.Date32Builder).Append(
					arrow.Date32FromTime(row[i].(time.Time)))
			case types.TIMESTAMP:
				ts, err := arrow.TimestampFromTime(row[i].(time.Time), arrow.Millisecond)
				if err != nil {
					return nil, err
				}
				rb.Field(i).(*array.TimestampBuilder).Append(ts)
			default:
				return nil, sparkerrors.WithType(fmt.Errorf(
					"unsupported data type: %s", field.DataType), sparkerrors.NotImplementedError)
			}
		}
	}
	rec := rb.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer tbl.Release()
	return s.SS.CreateDataFrameFromArrow(ctx, tbl)
}

func (s *SparkSQL) ExecQueryBatchProcessing(query string, batchSize int, function ...func(input []map[string]interface{}) error) (err error) {

	var (
		frame   sql.DataFrame
		collect []types.Row
	)

	ctx, cancelFunc := context.WithTimeout(context.Background(), s.Timeout)
	defer cancelFunc()

	frame, err = s.SS.Sql(ctx, query)
	if err != nil {
		return err
	}

	collect, err = frame.Collect(ctx)
	if err != nil {
		return err
	}

	var rows []map[string]interface{}
	for index, row := range collect {
		record := map[string]interface{}{}
		for _, name := range row.FieldNames() {
			record[name] = row.Value(name)
		}
		rows = append(rows, record)
		if (index+1)%batchSize == 0 {
			for _, fun := range function {
				err = fun(rows)
				if err != nil {
					return err
				}
			}
			rows = rows[:0]
		}
	}

	if len(rows) > 0 {
		for _, fun := range function {
			err = fun(rows)
			if err != nil {
				return err
			}
		}
		rows = rows[:0]
	}

	return nil
}
