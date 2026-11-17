package fsql

import (
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/grafana-influxdb-datasource/pkg/influxdb/models"
)

// oauthIdentityHeaders are the per-user headers Grafana forwards when the
// datasource has "Forward OAuth identity" enabled.
var oauthIdentityHeaders = []string{backend.OAuthIdentityTokenHeaderName, backend.OAuthIdentityIDTokenHeaderName}

// buildMetadata returns the gRPC metadata sent with every Flight SQL call of
// one request.
//
// Sources are applied in this order; a later source overrides an earlier one
// for the same key (keys are case-insensitive):
//  1. "database" and "authorization: Bearer <token>" from the datasource settings
//  2. custom headers configured on the datasource (httpHeaderName/httpHeaderValue),
//     passed through unfiltered
//  3. OAuth identity headers Grafana forwarded for the current user
//
// This matches the HTTP paths, where forwarded OAuth headers win over the
// configured credentials. Do not reorder without updating that behaviour too.
// forwarded may be nil.
func buildMetadata(dsInfo *models.DatasourceInfo, forwarded http.Header) metadata.MD {
	md := metadata.MD{}

	if dsInfo.DbName != "" {
		md.Set("database", dsInfo.DbName)
	}

	if dsInfo.Token != "" {
		md.Set("authorization", "Bearer "+dsInfo.Token)
	}

	for k, vs := range dsInfo.CustomHeaders {
		md.Set(k, vs...)
	}

	for _, k := range oauthIdentityHeaders {
		if v := forwarded.Get(k); v != "" {
			md.Set(k, v)
		}
	}

	return md
}
