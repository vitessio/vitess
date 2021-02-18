package grpctabletconn

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"

	"github.com/golang/protobuf/jsonpb"
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

const longSQL = `create table vitess_ints(tiny tinyint default 0, tinyu tinyint unsigned default null, small smallint default null, smallu smallint unsigned default null, medium mediumint default null, mediumu mediumint unsigned default null, normal int default null, normalu int unsigned default null, big bigint default null, bigu bigint unsigned default null, y year default null, primary key(tiny));`

type generator struct {
	n int
	r *rand.Rand
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

func TestRandomGeneration(t *testing.T) {
	gen := &generator{
		n: 50,
		r: rand.New(rand.NewSource(420)),
	}

	bq := gen.generateBoundQuery()

	m := jsonpb.Marshaler{}
	m.Indent = "  "
	s, _ := m.MarshalToString(&querypb.ExecuteBatchRequest{Queries: bq})
	t.Log(s)
}

func BenchmarkGRPCTabletConn(b *testing.B) {
	// fake service
	service := &BenchmarkService{
		t:           b,
		batchResult: tabletconntest.ExecuteBatchQueryResultList,
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

	for _, querySize := range []int{5, 10, 50, 100, 500, 1000, 5000, 25000} {
		b.Run(fmt.Sprintf("Proto-%d", querySize), func(b *testing.B) {
			conn, err := tabletconn.GetDialer()(tablet, false)
			if err != nil {
				b.Fatalf("dial failed: %v", err)
			}
			defer conn.Close(context.Background())

			gen := &generator{
				n: querySize,
				r: rand.New(rand.NewSource(int64(420 ^ querySize))),
			}
			query := gen.generateBoundQuery()

			b.SetParallelism(4)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ctx := context.Background()
					ctx = callerid.NewContext(ctx, tabletconntest.TestCallerID, tabletconntest.TestVTGateCallerID)
					_, err := conn.ExecuteBatch(ctx, tabletconntest.TestTarget, query, tabletconntest.TestAsTransaction, tabletconntest.ExecuteBatchTransactionID, tabletconntest.TestExecuteOptions)
					if err != nil {
						b.Fatalf("ExecuteBatch failed: %v", err)
					}
				}
			})
		})
	}
}
