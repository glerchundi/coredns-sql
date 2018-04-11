package query

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/coredns/coredns/plugin/pkg/tls"
	"github.com/go-sql-driver/mysql"
	"github.com/miekg/dns"
)

type mysqlQueryer struct {
	*sql.DB
}

func NewMySQLQueryer(u *url.URL, tlsArgs ...string) (Queryer, error) {
	if len(tlsArgs) > 0 {
		u.Query().Set("tls", "custom")
		tlsConfig, err := tls.NewTLSConfigFromArgs(tlsArgs...)
		if err != nil {
			return nil, err
		}
		mysql.RegisterTLSConfig("custom", tlsConfig)
	}

	dsn := fmt.Sprintf("%s@tcp(%s)%s?%s", u.User.String(), u.Host, u.Path, u.RawQuery)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return &mysqlQueryer{db}, nil
}

func (mq *mysqlQueryer) Query(q string, s ScanFunc) ([]dns.RR, error) {
	return query(mq.DB, q, s)
}
