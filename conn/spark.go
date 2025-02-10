// Package conn @author: Violet-Eva @date  : 2024/12/26 @notes :
package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/violet-eva-01/datalake/util"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"net"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type SparkSQL struct {
	sql.SparkSession
	ctx     context.Context
	builder *BaseBuilder
}

func SparkConnServer(ip string, port int, args map[string]string, ctxL ...context.Context) (*SparkSQL, error) {
	var (
		param    string
		remote   = fmt.Sprintf("sc://%s:%d", ip, port)
		builder  *BaseBuilder
		sparkSQL sql.SparkSession
		err      error
		ctx      context.Context
	)

	if len(ctxL) > 0 {
		ctx = ctxL[0]
	} else {
		ctx = context.Background()
	}

	if args != nil && len(args) > 0 {
		param = "/"
		for k, v := range args {
			param += fmt.Sprintf(";%s=%s", k, v)
		}
		remote += param
		builder, err = newBuilder(remote, args)
		if err != nil {
			return nil, err
		}
		sparkSQL, err = sql.NewSessionBuilder().WithChannelBuilder(builder).Build(ctx)
	} else {
		sparkSQL, err = sql.NewSessionBuilder().Remote(remote).Build(ctx)
	}
	if err != nil {
		return nil, err
	}
	_, err = sparkSQL.Sql(ctx, "select 1")
	if err != nil {
		return nil, err
	}
	return &SparkSQL{
		sparkSQL,
		ctx,
		builder,
	}, nil
}

func NewSparkSQL(ip string, port int, args map[string]string, retryTime int, retryInterval time.Duration, ctxL ...context.Context) (conn *SparkSQL, err error) {
	for i := 0; i < retryTime; i++ {
		conn, err = SparkConnServer(ip, port, args, ctxL...)
		if err != nil {
			if i != retryTime-1 {
				time.Sleep(retryInterval * time.Second)
				continue
			} else {
				return nil, fmt.Errorf("connect spark connect server failed ,err is %s", err)
			}
		}
	}
	return
}

func (s *SparkSQL) Stop() error {
	if s.builder != nil && s.builder.conn != nil {
		err := s.builder.conn.Close()
		return err
	}
	return nil
}

func (s *SparkSQL) Exec(query string) (sql.DataFrame, error) {
	return s.Sql(s.ctx, query)
}

// structToStructType
// @Description: isTag is false , get struct elem name assign to structField name. isTag is true  , get json tag name assign to structField name.
// @param v
// @param isTag
// @return *types.StructType
// @return error
func structToStructType(v interface{}, isRename bool) (*types.StructType, error) {
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
		switch vt := vf.Field(i).Interface().(type) {
		case int:
			switch runtime.GOARCH {
			case "386", "arm":
				filed.DataType = types.INTEGER
			default:
				filed.DataType = types.LONG
			}
		case bool:
			filed.DataType = types.BOOLEAN
		case int8:
			filed.DataType = types.BYTE
		case int16:
			filed.DataType = types.SHORT
		case int32:
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
			tag := tf.Field(i).Tag.Get("type")
			if tag == "timestamp" {
				filed.DataType = types.TIMESTAMP
			} else {
				filed.DataType = types.DATE
			}
		default:
			panic(fmt.Errorf("unsupported data type: %s", vt))
		}
		filed.Metadata = nil
		filed.Nullable = true
		fields = append(fields, filed)
	}
	return &types.StructType{
		Fields: fields,
	}, nil
}

// sAToTSA
// @Description: any slice  -> any 2D slicing
// @param structType
// @param data
// @return [][]interface{}
func sAToTSA(structType *types.StructType, data ...any) [][]interface{} {
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
func (s *SparkSQL) CreateDataFrameFromStruct(data any, isRename bool) (sql.DataFrame, error) {
	rows := anyToSliceAny(data)
	if len(rows) == 0 {
		return nil, fmt.Errorf("no data")
	}
	structType, err := structToStructType(rows[0], isRename)
	if err != nil {
		return nil, err
	}
	sliceAny := sAToTSA(structType, rows...)
	return s.createDataFrame(s.ctx, sliceAny, structType)
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
	var rows [][]interface{}
	for _, row := range data {
		var record []interface{}
		for i := 0; i < length; i++ {
			var rec interface{}
			if len(mappingTags) > 0 {
				rec = row[mappingTags[structType.Fields[i].Name]]
				if rec == nil && !isTag {
					rec = row[structType.Fields[i].Name]
				}
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
func (s *SparkSQL) CreateDataFrameFromMap(v interface{}, isTag bool, isRename bool, data ...map[string]interface{}) (sql.DataFrame, error) {
	structType, err := structToStructType(v, isRename)
	if err != nil {
		return nil, err
	}
	sliceAny, err := mapToSliceAny(structType, v, isTag, isRename, data...)
	if err != nil {
		return nil, err
	}
	return s.createDataFrame(s.ctx, sliceAny, structType)
}

func (s *SparkSQL) createDataFrame(ctx context.Context, data [][]any, schema *types.StructType) (sql.DataFrame, error) {
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
				switch row[i].(type) {
				case int:
					rb.Field(i).(*array.Int32Builder).Append(int32(row[i].(int)))
				default:
					rb.Field(i).(*array.Int32Builder).Append(row[i].(int32))
				}
			case types.LONG:
				switch row[i].(type) {
				case int:
					rb.Field(i).(*array.Int64Builder).Append(int64(row[i].(int)))
				default:
					rb.Field(i).(*array.Int64Builder).Append(row[i].(int64))
				}
			case types.FLOAT:
				rb.Field(i).(*array.Float32Builder).Append(row[i].(float32))
			case types.DOUBLE:
				rb.Field(i).(*array.Float64Builder).Append(row[i].(float64))
			case types.STRING:
				rb.Field(i).(*array.StringBuilder).Append(row[i].(string))
			case types.DATE:
				rb.Field(i).(*array.Date32Builder).Append(arrow.Date32FromTime(row[i].(time.Time)))
			// case filed , err is execution error: [Internal] [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(MILLISECOND, UTC).
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
	return s.CreateDataFrameFromArrow(ctx, tbl)
}

func (s *SparkSQL) ExecQuery(query string) (output []map[string]interface{}, err error) {

	var (
		frame   sql.DataFrame
		collect []types.Row
	)

	frame, err = s.Exec(query)
	if err != nil {
		return
	}
	defer func() {
		frame = nil
	}()
	collect, err = frame.Collect(s.ctx)
	if err != nil {
		return
	}

	var rows []map[string]interface{}
	for _, row := range collect {
		record := make(map[string]interface{})
		for _, name := range row.FieldNames() {
			record[name] = row.Value(name)
		}
		rows = append(rows, record)
	}
	return
}

func (s *SparkSQL) ExecQueryToMapString(query string) (output []map[string]string, err error) {

	var (
		frame   sql.DataFrame
		collect []types.Row
	)

	frame, err = s.Exec(query)
	if err != nil {
		return
	}
	defer func() {
		frame = nil
	}()

	collect, err = frame.Collect(s.ctx)
	if err != nil {
		return
	}

	var rows []map[string]string
	for _, row := range collect {
		record := make(map[string]string)
		for _, name := range row.FieldNames() {
			record[name] = fmt.Sprintf("%s", row.Value(name))
		}
		rows = append(rows, record)
	}

	return
}

func (s *SparkSQL) dFCollectBatchProcessingForString(df sql.DataFrame, batchSize int, function ...func(input []map[string]string) error) (err error) {
	defer func() {
		df = nil
	}()
	var collect []types.Row

	collect, err = df.Collect(s.ctx)
	if err != nil {
		return err
	}

	var rows []map[string]string
	for index, row := range collect {
		record := make(map[string]string)
		for _, name := range row.FieldNames() {
			record[name] = fmt.Sprintf("%s", row.Value(name))
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

	return
}

func (s *SparkSQL) ExecQueryBatchProcessingForString(query string, batchSize int, function ...func(input []map[string]string) error) (err error) {

	var frame sql.DataFrame

	frame, err = s.Exec(query)
	if err != nil {
		return err
	}

	return s.dFCollectBatchProcessingForString(frame, batchSize, function...)
}

func (s *SparkSQL) dFCollectBatchProcessingForInterface(df sql.DataFrame, batchSize int, function ...func(input []map[string]interface{}) error) (err error) {
	defer func() {
		df = nil
	}()
	var collect []types.Row

	collect, err = df.Collect(s.ctx)
	if err != nil {
		return err
	}
	var rows []map[string]interface{}
	for index, row := range collect {
		record := make(map[string]interface{})
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

	df = nil
	return
}

func (s *SparkSQL) ExecQueryBatchProcessingForInterface(query string, batchSize int, function ...func(input []map[string]interface{}) error) (err error) {

	var (
		frame sql.DataFrame
	)

	frame, err = s.Exec(query)
	if err != nil {
		return err
	}

	return s.dFCollectBatchProcessingForInterface(frame, batchSize, function...)
}

type BaseBuilder struct {
	host      string
	port      int
	token     string
	user      string
	headers   map[string]string
	sessionId string
	conn      *grpc.ClientConn
}

func (cb *BaseBuilder) Host() string {
	return cb.host
}

func (cb *BaseBuilder) Port() int {
	return cb.port
}

func (cb *BaseBuilder) Token() string {
	return cb.token
}

func (cb *BaseBuilder) User() string {
	return cb.user
}

func (cb *BaseBuilder) Headers() map[string]string {
	return cb.headers
}

func (cb *BaseBuilder) SessionId() string {
	return cb.sessionId
}

func newBuilder(connection string, args map[string]string) (*BaseBuilder, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}

	if u.Hostname() == "" {
		return nil, sparkerrors.WithType(errors.New("URL must contain a hostname"), sparkerrors.InvalidInputError)
	}

	if u.Scheme != "sc" {
		return nil, sparkerrors.WithType(errors.New("URL schema must be set to `sc`"), sparkerrors.InvalidInputError)
	}

	port := 15002
	host := u.Host
	// Check if the host part of the URL contains a port and extract.
	if strings.Contains(u.Host, ":") {
		// We can ignore the error here already since the url parsing
		// raises the error about invalid port.
		hostStr, portStr, _ := net.SplitHostPort(u.Host)
		host = hostStr
		if len(portStr) != 0 {
			port, err = strconv.Atoi(portStr)
			if err != nil {
				return nil, err
			}
		}
	}

	// Validate that the URL path is empty or follows the right format.
	if u.Path != "" && !strings.HasPrefix(u.Path, "/;") {
		return nil, sparkerrors.WithType(
			fmt.Errorf("the URL path (%v) must be empty or have a proper parameter syntax", u.Path),
			sparkerrors.InvalidInputError)
	}

	cb := &BaseBuilder{
		host: host,
		port: port,
	}

	var headers = make(map[string]string)
	for k, v := range args {
		switch k {
		case "user_id":
			cb.user = v
		case "session_d":
			cb.sessionId = v
		case "token":
			cb.token = v
		default:
			headers[k] = v
		}
	}
	cb.headers = headers

	return cb, nil
}

func (cb *BaseBuilder) Build(ctx context.Context) (*grpc.ClientConn, error) {
	var (
		opts []grpc.DialOption
	)

	opts = append(opts, grpc.WithAuthority(cb.host))
	if cb.token == "" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		// Note: On the Windows platform, use of x509.SystemCertPool() requires
		// go version 1.18 or higher.
		systemRoots, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		cred := credentials.NewTLS(&tls.Config{
			RootCAs: systemRoots,
		})
		opts = append(opts, grpc.WithTransportCredentials(cred))
		ts := oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: cb.token,
			TokenType:   "bearer",
		})
		opts = append(opts, grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: ts}))
	}

	remote := fmt.Sprintf("%v:%v", cb.host, cb.port)
	conn, err := grpc.NewClient(remote, opts...)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w",
			remote, err), sparkerrors.ConnectionError)
	}
	cb.conn = conn
	return conn, nil
}
