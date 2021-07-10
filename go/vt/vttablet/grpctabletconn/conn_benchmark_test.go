package grpctabletconn

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"testing"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
)

type BenchmarkService struct {
	t           testing.TB
	batchResult []sqltypes.Result
}

func (b *BenchmarkService) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, *topodatapb.TabletAlias, error) {
	panic("should not be called")
}

func (b *BenchmarkService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	panic("should not be called")
}

func (b *BenchmarkService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	panic("should not be called")
}

func (b *BenchmarkService) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	panic("should not be called")
}

func (b *BenchmarkService) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	panic("should not be called")
}

func (b *BenchmarkService) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	panic("should not be called")
}

func (b *BenchmarkService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	panic("should not be called")
}

func (b *BenchmarkService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return b.batchResult, nil
}

func (b *BenchmarkService) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	panic("should not be called")
}

func (b *BenchmarkService) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	panic("should not be called")
}

func (b *BenchmarkService) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	panic("should not be called")
}

func (b *BenchmarkService) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	panic("should not be called")
}

func (b *BenchmarkService) VStream(ctx context.Context, target *querypb.Target, startPos string, tableLastPKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	panic("should not be called")
}

func (b *BenchmarkService) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	panic("should not be called")
}

func (b *BenchmarkService) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	panic("should not be called")
}

func (b *BenchmarkService) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	panic("should not be called")
}

func (b *BenchmarkService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("caught test panic: %v", x)
	}
}

func (b *BenchmarkService) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {
	panic("should not be called")
}

func (b *BenchmarkService) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	panic("should not be called")
}

func (b *BenchmarkService) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	panic("should not be called")
}

func (b *BenchmarkService) Close(ctx context.Context) error {
	panic("should not be called")
}

type generator struct {
	n  int
	r  *rand.Rand
	tt []querypb.Type
}

func (gen *generator) randomString(min, max int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	n := min + gen.r.Intn(max-min)
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[gen.r.Intn(len(letters))]
	}
	return string(b)
}

func (gen *generator) generateBindVar(tt int) interface{} {
	if gen.n < 0 {
		return nil
	}
	gen.n--

	switch tt {
	case 0:
		return gen.randomString(1, 256)
	case 1:
		return gen.r.Float64()
	case 2:
		return gen.r.Uint64()
	case 3:
		return gen.r.Int63()
	case 4, 5, 6, 7:
		var (
			ary []interface{}
			l   = gen.r.Intn(32)
		)
		for i := 0; i < l && gen.n > 0; i++ {
			ary = append(ary, gen.generateBindVar(tt-4))
		}
		return ary
	default:
		panic("unreachable")
	}
}

func (gen *generator) generateBindVars() map[string]*querypb.BindVariable {
	bv := make(map[string]*querypb.BindVariable)
	c := gen.r.Intn(128)
	for i := 0; i < c && gen.n > 0; i++ {
		tt := gen.r.Intn(8)
		v, err := sqltypes.BuildBindVariable(gen.generateBindVar(tt))
		if err != nil {
			panic(err)
		}
		bv[fmt.Sprintf("bind%d", i)] = v
	}
	return bv
}

func (gen *generator) generateBoundQuery() (bq []*querypb.BoundQuery) {
	for gen.n > 0 {
		q := &querypb.BoundQuery{
			Sql:           gen.randomString(32, 1024),
			BindVariables: gen.generateBindVars(),
		}
		gen.n--
		bq = append(bq, q)
	}
	return
}

func (gen *generator) generateType() querypb.Type {
	if len(gen.tt) == 0 {
		for _, tt := range querypb.Type_value {
			gen.tt = append(gen.tt, querypb.Type(tt))
		}
		sort.Slice(gen.tt, func(i, j int) bool {
			return gen.tt[i] < gen.tt[j]
		})
	}
	return gen.tt[gen.r.Intn(len(gen.tt))]
}

func (gen *generator) generateRows() (fields []*querypb.Field, rows [][]sqltypes.Value) {
	fieldCount := 1 + gen.r.Intn(16)

	for i := 0; i < fieldCount; i++ {
		fields = append(fields, &querypb.Field{
			Name: fmt.Sprintf("field%d", i),
			Type: gen.generateType(),
		})
	}

	for gen.n > 0 {
		var row []sqltypes.Value
		for _, f := range fields {
			row = append(row, sqltypes.TestValue(f.Type, gen.randomString(8, 32)))
			gen.n--
		}
		rows = append(rows, row)
	}
	return
}

func (gen *generator) generateQueryResultList() (qrl []sqltypes.Result) {
	for gen.n > 0 {
		fields, rows := gen.generateRows()
		r := sqltypes.Result{
			Fields:       fields,
			RowsAffected: gen.r.Uint64(),
			InsertID:     gen.r.Uint64(),
			Rows:         rows,
		}
		gen.n--
		qrl = append(qrl, r)
	}
	return
}

func BenchmarkGRPCTabletConn(b *testing.B) {
	// fake service
	service := &BenchmarkService{
		t: b,
	}

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		b.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcqueryservice.Register(server, service)
	go server.Serve(listener)

	tablet := &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Alias:    tabletconntest.TestAlias,
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}

	var querySizes = []int{8, 64, 512, 4096, 32768}
	var requests = make(map[int][]*querypb.BoundQuery)
	var responses = make(map[int][]sqltypes.Result)

	for _, size := range querySizes {
		gen := &generator{
			n: size,
			r: rand.New(rand.NewSource(int64(0x33333 ^ size))),
		}
		requests[size] = gen.generateBoundQuery()

		gen = &generator{
			n: size,
			r: rand.New(rand.NewSource(int64(0x44444 ^ size))),
		}
		responses[size] = gen.generateQueryResultList()
	}

	for _, reqSize := range querySizes {
		for _, respSize := range querySizes {
			b.Run(fmt.Sprintf("Req%d-Resp%d", reqSize, respSize), func(b *testing.B) {
				conn, err := tabletconn.GetDialer()(tablet, false)
				if err != nil {
					b.Fatalf("dial failed: %v", err)
				}
				defer conn.Close(context.Background())

				requestQuery := requests[reqSize]
				service.batchResult = responses[respSize]

				b.SetParallelism(4)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						ctx := context.Background()
						ctx = callerid.NewContext(ctx, tabletconntest.TestCallerID, tabletconntest.TestVTGateCallerID)
						_, err := conn.ExecuteBatch(ctx, tabletconntest.TestTarget, requestQuery, tabletconntest.TestAsTransaction, tabletconntest.ExecuteBatchTransactionID, tabletconntest.TestExecuteOptions)
						if err != nil {
							b.Fatalf("ExecuteBatch failed: %v", err)
						}
					}
				})
			})
		}
	}
}
