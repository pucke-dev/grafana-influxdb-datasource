package fsql

import (
	"net/http"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/grafana-influxdb-datasource/pkg/influxdb/models"
)

func TestBuildMetadata(t *testing.T) {
	oauth := func(token, idToken string) http.Header {
		h := http.Header{}
		if token != "" {
			h.Set(backend.OAuthIdentityTokenHeaderName, token)
		}
		if idToken != "" {
			h.Set(backend.OAuthIdentityIDTokenHeaderName, idToken)
		}
		return h
	}

	tests := []struct {
		name      string
		dsInfo    models.DatasourceInfo
		forwarded http.Header
		expected  metadata.MD
	}{
		{
			name:     "token is sent as bearer",
			dsInfo:   models.DatasourceInfo{Token: "secret"},
			expected: metadata.MD{"authorization": {"Bearer secret"}},
		},
		{
			name:     "database is set",
			dsInfo:   models.DatasourceInfo{DbName: "influxdb", Token: "secret"},
			expected: metadata.MD{"database": {"influxdb"}, "authorization": {"Bearer secret"}},
		},
		{
			name:     "nothing configured yields empty metadata",
			expected: metadata.MD{},
		},
		{
			name: "custom headers are lowercased and keep all values",
			dsInfo: models.DatasourceInfo{CustomHeaders: http.Header{
				"X-Org": {"a", "b"},
			}},
			expected: metadata.MD{"x-org": {"a", "b"}},
		},
		{
			name: "custom authorization overrides token",
			dsInfo: models.DatasourceInfo{
				Token:         "secret",
				CustomHeaders: http.Header{"Authorization": {"Token custom"}},
			},
			expected: metadata.MD{"authorization": {"Token custom"}},
		},
		{
			name: "forwarded oauth overrides custom header and token",
			dsInfo: models.DatasourceInfo{
				Token:         "secret",
				CustomHeaders: http.Header{"Authorization": {"Token custom"}},
			},
			forwarded: oauth("Bearer user", "id-token"),
			expected: metadata.MD{
				"authorization": {"Bearer user"},
				"x-id-token":    {"id-token"},
			},
		},
		{
			name:      "forwarded id token alone keeps configured token",
			dsInfo:    models.DatasourceInfo{Token: "secret"},
			forwarded: oauth("", "id-token"),
			expected: metadata.MD{
				"authorization": {"Bearer secret"},
				"x-id-token":    {"id-token"},
			},
		},
		{
			name:      "non-oauth forwarded headers are ignored",
			forwarded: http.Header{"Cookie": {"session=1"}, "X-Other": {"v"}},
			expected:  metadata.MD{},
		},
		{
			name:     "nil forwarded and nil custom headers are fine",
			dsInfo:   models.DatasourceInfo{Token: "secret"},
			expected: metadata.MD{"authorization": {"Bearer secret"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, buildMetadata(&tt.dsInfo, tt.forwarded))
		})
	}
}
