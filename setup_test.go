package database

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/mholt/caddy"
	"github.com/miekg/dns"
)

func TestDatabaseParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *DatabaseConfig
		wantErr bool
	}{
		{
			name:    "empty",
			input:   "database",
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid url",
			input: `database example.org {
	url ##
}`,
			want:    nil,
			wantErr: true,
		},
		{
			name: "smallest",
			input: `database example.org {
	url postgresql://ip:1234/db
}`,
			want: &DatabaseConfig{
				Zones:   make([]string, 0),
				URL:     newURL("postgresql://127.0.0.1/coredns"),
				Queries: defaultQueries,
			},
			wantErr: false,
		},
		{
			name: "tls",
			input: `database example.org {
	url postgresql://ip:1234/db
	tls client.crt client.key ca.pem
}`,
			want: &DatabaseConfig{
				Zones:   make([]string, 0),
				URL:     newURL("postgresql://127.0.0.1/coredns"),
				Queries: defaultQueries,
				TLSArgs: []string{"client.crt", "client.key", "ca.pem"},
			},
			wantErr: false,
		},
		{
			name: "tls (ca-only)",
			input: `database example.org {
	url postgresql://ip:1234/db
	tls ca.pem
}`,
			want: &DatabaseConfig{
				Zones:   make([]string, 0),
				URL:     newURL("mysql://127.0.0.1:3306/coredns"),
				Queries: defaultQueries,
				TLSArgs: []string{"ca.pem"},
			},
			wantErr: false,
		},
		{
			name: "custom queries (override a)",
			input: `database {
	url postgresql://ip:1234/db
	a_query "SELECT name, ttl, addr FROM tbl_a WHERE name = '{{.Name}}'"
}`,
			want: &DatabaseConfig{
				Zones: make([]string, 0),
				URL:   newURL("postgresql://ip:1234/db"),
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
			input: `database example.org {
	url postgresql://ip:1234/db
	a_query SELECT name, ttl, addr FROM tbl_a WHERE name = '{{.Name}}'
}`,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := databaseParse(caddy.NewTestController("database", tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("databaseConfigParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("databaseConfigParse() = %v, want %v", got, tt.want)
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
