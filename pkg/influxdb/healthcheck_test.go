package influxdb

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/example"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/proxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/grafana-influxdb-datasource/pkg/influxdb/models"
)

func TestIntegrationSQLHealthcheckForwardsHeaders(t *testing.T) {
	if !strings.HasPrefix(t.Name(), "TestIntegration") {
		t.Fatal("test is not an integration test")
	}
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	db, err := example.CreateDB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	sqliteServer, err := example.NewSQLiteFlightSQLServer(db)
	require.NoError(t, err)

	var (
		mu       sync.Mutex
		received metadata.MD
	)
	recordAuth := flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			mu.Lock()
			received = md.Copy()
			mu.Unlock()
			return handler(ctx, req)
		},
	}

	server := flight.NewServerWithMiddleware([]flight.ServerMiddleware{recordAuth})
	server.RegisterFlightService(flightsql.NewFlightServer(sqliteServer))
	require.NoError(t, server.Init("127.0.0.1:0"))
	go func() { _ = server.Serve() }()
	t.Cleanup(server.Shutdown)

	ds := &DataSource{info: &models.DatasourceInfo{
		URL:          "http://" + server.Addr().String(),
		Token:        "configured-token",
		Version:      influxVersionSQL,
		InsecureGrpc: true,
		ProxyClient:  proxy.New(nil),
	}}

	res, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
		Headers: map[string]string{
			backend.OAuthIdentityTokenHeaderName:   "Bearer user-token",
			backend.OAuthIdentityIDTokenHeaderName: "user-id-token",
		},
	})
	require.NoError(t, err)
	require.Equal(t, backend.HealthStatusOk, res.Status, res.Message)

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, received, "flight server received no unary call")
	require.Equal(t, []string{"Bearer user-token"}, received.Get("authorization"))
	require.Equal(t, []string{"user-id-token"}, received.Get("x-id-token"))
}

func Test_healthcheck(t *testing.T) {
	t.Run("should do successful health check for version flux ", func(t *testing.T) {
		ds := GetMockDataSource(influxVersionFlux, RoundTripper{
			Body: `#datatype,string,long,string,string,string,string,long
#group,false,false,false,false,true,false,false
#default,_result,,,,,,
,result,table,name,id,organizationID,retentionPolicy,retentionPeriod
,,0,_monitoring,effbe6d547e1c085,c678d3a458299f4e,,604800000000000
,,0,_tasks,9ac37d3047b0970c,c678d3a458299f4e,,259200000000000
,,0,mybucket,98184c45c69fc01e,c678d3a458299f4e,,0`,
		})
		res, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
			PluginContext: backend.PluginContext{},
			Headers:       nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, backend.HealthStatusOk, res.Status)
	})
	t.Run("should do successful health check for version InfluxQL", func(t *testing.T) {
		ds := GetMockDataSource(influxVersionInfluxQL, RoundTripper{
			Body: `{"results": [{"series": [{"columns": ["name"],"name": "measurements","values": [["cpu"],["disk"],["diskio"],["kernel"],["mem"],["processes"],["swap"],["system"]]}],"statement_id": 0}]}`,
		})
		res, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
			PluginContext: backend.PluginContext{},
			Headers:       nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, backend.HealthStatusOk, res.Status)
	})
	t.Run("should fail when version is unknown", func(t *testing.T) {
		ds := GetMockDataSource("unknown-influx-version", RoundTripper{
			Body: `{"results": [{"series": [{"columns": ["name"],"name": "measurements","values": [["cpu"],["disk"],["diskio"],["kernel"],["mem"],["processes"],["swap"],["system"]]}],"statement_id": 0}]}`,
		})
		res, _ := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
			PluginContext: backend.PluginContext{},
			Headers:       nil,
		})
		assert.Equal(t, backend.HealthStatusError, res.Status)
	})
}
