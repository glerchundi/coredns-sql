package sql

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/mholt/caddy"
	"github.com/miekg/dns"
)

func TestSQLParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *SQLConfig
		wantErr bool
	}{
		{
			name:    "empty",
			input:   "sql",
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid url",
			input: `sql example.org {
	url ##
}`,
			want:    nil,
			wantErr: true,
		},
		{
			name: "smallest",
			input: `sql example.org {
	url postgresql://127.0.0.1/coredns
}`,
			want: &SQLConfig{
				Zones:   []string{"example.org."},
				URL:     newURL("postgresql://127.0.0.1/coredns"),
				Queries: defaultQueries,
			},
			wantErr: false,
		},
		{
			name: "tls",
			input: `sql example.org {
	url postgresql://127.0.0.1/coredns
	tls client.crt client.key ca.pem
}`,
			want: &SQLConfig{
				Zones:   []string{"example.org."},
				URL:     newURL("postgresql://127.0.0.1/coredns"),
				Queries: defaultQueries,
				TLSArgs: []string{"client.crt", "client.key", "ca.pem"},
			},
			wantErr: false,
		},
		{
			name: "tls (ca-only)",
			input: `sql example.org {
	url mysql://127.0.0.1:3306/coredns
	tls ca.pem
}`,
			want: &SQLConfig{
				Zones:   []string{"example.org."},
				URL:     newURL("mysql://127.0.0.1:3306/coredns"),
				Queries: defaultQueries,
				TLSArgs: []string{"ca.pem"},
			},
			wantErr: false,
		},
		{
			name: "custom queries (override a)",
			input: `sql example.org {
	url postgresql://127.0.0.1/coredns
	a_query "SELECT name, ttl, addr FROM tbl_a WHERE name = '{{.Name}}'"
}`,
			want: &SQLConfig{
				Zones: []string{"example.org."},
				URL:   newURL("postgresql://127.0.0.1/coredns"),
				Queries: map[uint16]string{
					dns.TypeA:     "SELECT name, ttl, addr FROM tbl_a WHERE name = '{{.Name}}'",
					dns.TypeAAAA:  defaultQueries[dns.TypeAAAA],
					dns.TypeCNAME: defaultQueries[dns.TypeCNAME],
				},
			},
			wantErr: false,
		},
		{
			name: "invalid custom query (no quotes)",
			input: `sql example.org {
	url postgresql://127.0.0.1/coredns
	a_query SELECT name, ttl, addr FROM tbl_a WHERE name = '{{.Name}}'
}`,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sqlParse(caddy.NewTestController(name, tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("sqlParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sqlParse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func newURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}
