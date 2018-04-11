package query

import (
	"database/sql"
	"net/url"

	_ "github.com/lib/pq"
	"github.com/miekg/dns"
)

type postgresQueryer struct {
	*sql.DB
}

func NewPostgresQueryer(u *url.URL, tlsArgs ...string) (Queryer, error) {
	q := u.Query()
	sslmodeUnset := q.Get("sslmode") == ""
	switch len(tlsArgs) {
	case 1:
		if sslmodeUnset {
			q.Set("sslmode", "verify-ca")
		}
		q.Set("sslrootcert", tlsArgs[0])
	case 2:
		q.Set("sslcert", tlsArgs[0])
		q.Set("sslkey", tlsArgs[1])
	case 3:
		if sslmodeUnset {
			q.Set("sslmode", "verify-full")
		}
		q.Set("sslcert", tlsArgs[0])
		q.Set("sslkey", tlsArgs[1])
		q.Set("sslrootcert", tlsArgs[2])
	}

	db, err := sql.Open("postgres", u.String())
	if err != nil {
		return nil, err
	}

	return &postgresQueryer{db}, nil
}

func (pq *postgresQueryer) Query(q string, s ScanFunc) ([]dns.RR, error) {
	return query(pq.DB, q, s)
}
