package fsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/example"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/proxy"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/grafana-influxdb-datasource/pkg/influxdb/models"
)

type FSQLTestSuite struct {
	suite.Suite
	db       *sql.DB
	server   flight.Server
	addr     string
	recorder *mdRecorder
}

const (
	getFlightInfoMethod = "/arrow.flight.protocol.FlightService/GetFlightInfo"
	doGetMethod         = "/arrow.flight.protocol.FlightService/DoGet"
)

// mdRecorder stores the last incoming metadata per gRPC method.
type mdRecorder struct {
	mu    sync.Mutex
	calls map[string]metadata.MD
}

func newMDRecorder() *mdRecorder {
	return &mdRecorder{calls: map[string]metadata.MD{}}
}

func (r *mdRecorder) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[method] = md.Copy()
}

func (r *mdRecorder) get(method string) metadata.MD {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[method]
}

func (r *mdRecorder) middleware() flight.ServerMiddleware {
	return flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			r.record(ctx, info.FullMethod)
			return handler(ctx, req)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			r.record(ss.Context(), info.FullMethod)
			return handler(srv, ss)
		},
	}
}

func (suite *FSQLTestSuite) SetupTest() {
	addr, _ := freeport(suite.T())

	suite.addr = addr
	suite.recorder = newMDRecorder()

	db, err := example.CreateDB()
	require.NoError(suite.T(), err)

	sqliteServer, err := example.NewSQLiteFlightSQLServer(db)
	require.NoError(suite.T(), err)
	sqliteServer.Alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)
	server := flight.NewServerWithMiddleware([]flight.ServerMiddleware{suite.recorder.middleware()})
	server.RegisterFlightService(flightsql.NewFlightServer(sqliteServer))
	err = server.Init(suite.addr)
	require.NoError(suite.T(), err)
	go func() {
		err := server.Serve()
		require.NoError(suite.T(), err)
	}()
	suite.db = db
	suite.server = server
}

func (suite *FSQLTestSuite) AfterTest(suiteName, testName string) {
	err := suite.db.Close()
	require.NoError(suite.T(), err)
	suite.server.Shutdown()
}

func TestIntegrationFSQLTestSuite(t *testing.T) {
	if !strings.HasPrefix(t.Name(), "TestIntegration") {
		t.Fatal("test is not an integration test")
	}
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	suite.Run(t, new(FSQLTestSuite))
}

func (suite *FSQLTestSuite) newExecutor() *Executor {
	executor, err := NewExecutor(&models.DatasourceInfo{
		HTTPClient:    nil,
		Token:         "secret",
		URL:           "http://" + suite.addr,
		DbName:        "influxdb",
		Version:       "test",
		HTTPMode:      "proxy",
		InsecureGrpc:  true,
		ProxyClient:   proxy.New(nil),
		CustomHeaders: http.Header{},
	}, http.Header{})
	require.NoError(suite.T(), err)
	return executor
}

func (suite *FSQLTestSuite) TestIntegration_QueryData() {
	suite.Run("should run simple query data", func() {
		executor := suite.newExecutor()
		defer func() { require.NoError(suite.T(), executor.Close()) }()

		respA := executor.Execute(context.Background(), backend.DataQuery{
			RefID: "A",
			JSON:  mustQueryJSON(suite.T(), "A", "select * from intTable"),
		})
		respB := executor.Execute(context.Background(), backend.DataQuery{
			RefID: "B",
			JSON:  mustQueryJSON(suite.T(), "B", "select 1"),
		})
		require.NoError(suite.T(), respB.Error)

		require.NoError(suite.T(), respA.Error)
		frame := respA.Frames[0]

		require.Equal(suite.T(), "id", frame.Fields[0].Name)
		require.Equal(suite.T(), "keyName", frame.Fields[1].Name)
		require.Equal(suite.T(), "value", frame.Fields[2].Name)
		require.Equal(suite.T(), "foreignId", frame.Fields[3].Name)
		for _, f := range frame.Fields {
			require.Equal(suite.T(), 4, f.Len())
		}
	})
}

func (suite *FSQLTestSuite) TestIntegration_SendsMetadata() {
	run := func(customHeaders, forwarded http.Header) {
		executor, err := NewExecutor(&models.DatasourceInfo{
			Token:         "secret",
			URL:           "http://" + suite.addr,
			DbName:        "influxdb",
			InsecureGrpc:  true,
			ProxyClient:   proxy.New(nil),
			CustomHeaders: customHeaders,
		}, forwarded)
		require.NoError(suite.T(), err)
		defer func() { require.NoError(suite.T(), executor.Close()) }()

		resp := executor.Execute(context.Background(), backend.DataQuery{
			RefID: "A",
			JSON:  mustQueryJSON(suite.T(), "A", "select 1"),
		})
		require.NoError(suite.T(), resp.Error)
	}

	assertMD := func(key, want string) {
		for _, method := range []string{getFlightInfoMethod, doGetMethod} {
			md := suite.recorder.get(method)
			require.NotNil(suite.T(), md, "no call recorded for %s", method)
			require.Equal(suite.T(), []string{want}, md.Get(key), "%s on %s", key, method)
		}
	}

	suite.Run("token and database", func() {
		run(nil, nil)
		assertMD("authorization", "Bearer secret")
		assertMD("database", "influxdb")
	})

	suite.Run("custom headers", func() {
		run(http.Header{"X-Org": {"grafana"}, "Authorization": {"Token custom"}}, nil)
		assertMD("x-org", "grafana")
		assertMD("authorization", "Token custom")
	})

	suite.Run("forwarded oauth identity", func() {
		forwarded := http.Header{}
		forwarded.Set(backend.OAuthIdentityTokenHeaderName, "Bearer user-token")
		forwarded.Set(backend.OAuthIdentityIDTokenHeaderName, "user-id-token")
		run(http.Header{"Authorization": {"Token custom"}}, forwarded)
		assertMD("authorization", "Bearer user-token")
		assertMD("x-id-token", "user-id-token")
	})
}

func (suite *FSQLTestSuite) TestIntegration_ConcurrentExecute() {
	executor := suite.newExecutor()
	defer func() { require.NoError(suite.T(), executor.Close()) }()

	var wg sync.WaitGroup
	responses := make([]backend.DataResponse, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			responses[idx] = executor.Execute(context.Background(), backend.DataQuery{
				RefID: fmt.Sprintf("Q%d", idx),
				JSON:  mustQueryJSON(suite.T(), fmt.Sprintf("Q%d", idx), "select * from intTable"),
			})
		}(i)
	}
	wg.Wait()

	for i, res := range responses {
		require.NoError(suite.T(), res.Error, "query %d failed", i)
		require.NotEmpty(suite.T(), res.Frames, "query %d returned no frames", i)
	}
}

func (suite *FSQLTestSuite) TestIntegration_QueryDataBatchContinuesAfterError() {
	executor := suite.newExecutor()
	defer func() { require.NoError(suite.T(), executor.Close()) }()

	respA := executor.Execute(context.Background(), backend.DataQuery{
		RefID: "A",
		JSON:  mustQueryJSON(suite.T(), "A", "select * from table_that_does_not_exist"),
	})
	respB := executor.Execute(context.Background(), backend.DataQuery{
		RefID: "B",
		JSON:  mustQueryJSON(suite.T(), "B", "select 1"),
	})

	require.Error(suite.T(), respA.Error, "the failing query must report its error")
	require.NoError(suite.T(), respB.Error, "the failing query must not abandon the batch")
	require.NotEmpty(suite.T(), respB.Frames)
}

func mustQueryJSON(t *testing.T, refID, sql string) []byte {
	t.Helper()

	b, err := json.Marshal(queryRequest{
		RefID:    refID,
		RawQuery: sql,
		Format:   "table",
	})
	if err != nil {
		panic(err)
	}
	return b
}

func freeport(t *testing.T) (addr string, err error) {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = l.Close()
	}()
	a := l.Addr().(*net.TCPAddr)
	return a.String(), nil
}

func TestInvalidSchema(t *testing.T) {
	executor, err := NewExecutor(&models.DatasourceInfo{
		HTTPClient:    nil,
		Token:         "secret",
		URL:           "http://127.0.0.1:1234",
		DbName:        "influxdb",
		Version:       "test",
		HTTPMode:      "proxy",
		InsecureGrpc:  true,
		ProxyClient:   proxy.New(nil),
		CustomHeaders: http.Header{},
	}, http.Header{})
	require.NoError(t, err)
	defer func() { require.NoError(t, executor.Close()) }()

	resp := executor.Execute(context.Background(), backend.DataQuery{
		RefID: "A",
		JSON:  []byte(`this is not valid JSON`),
	})
	require.Equal(t, backend.ErrorSourceDownstream, resp.ErrorSource)
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "empty URL",
			input:    "",
			expected: "",
			hasError: true,
		},
		{
			name:     "URL without scheme",
			input:    "example.com",
			expected: "example.com",
			hasError: false,
		},
		{
			name:     "URL without scheme and with port",
			input:    "example.com:8181",
			expected: "example.com:8181",
			hasError: false,
		},
		{
			name:     "URL without port",
			input:    "http://example.com",
			expected: "example.com:443",
			hasError: false,
		},
		{
			name:     "URL with http scheme",
			input:    "http://example.com:8080",
			expected: "example.com:8080",
			hasError: false,
		},
		{
			name:     "URL with https scheme",
			input:    "https://example.com:8443",
			expected: "example.com:8443",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseURL(tt.input)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestErrStrIsDownstream(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "query failed is classified downstream",
			input:    "code = Internal desc = query failed: Invalid Error: Failed to query most recent snapshot",
			expected: true,
		},
		{
			name:     "no query failed is not classified downstream",
			input:    "unknown error no failed query here",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, errStrIsDownstream(tt.input))
		})
	}
}
