package query

import (
	"database/sql"
	"fmt"
	"net"

	"github.com/miekg/dns"
)

type ScanFunc func(*sql.Rows) (dns.RR, error)

var (
	scan = map[uint16]ScanFunc{
		dns.TypeA:     scanA,
		dns.TypeCNAME: scanCNAME,
	}
)

type Queryer interface {
	Query(query string, scan ScanFunc) ([]dns.RR, error)
}

func Scan(qtype uint16) ScanFunc {
	return scan[qtype]
}

func scanA(rows *sql.Rows) (dns.RR, error) {
	var ttl uint32
	var name, addr string
	err := rows.Scan(&name, &ttl, &addr)
	if err != nil {
		return nil, err
	}

	return &dns.A{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl},
		A:   net.ParseIP(addr),
	}, nil
}

func scanCNAME(rows *sql.Rows) (dns.RR, error) {
	var ttl uint32
	var name, target string
	err := rows.Scan(&name, &ttl, &target)
	if err != nil {
		return nil, err
	}

	return &dns.CNAME{
		Hdr:    dns.RR_Header{Name: name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: ttl},
		Target: target,
	}, nil
}

func query(db *sql.DB, query string, scan ScanFunc) ([]dns.RR, error) {
	if scan == nil {
		return nil, fmt.Errorf("scan shouldn't be nil, this is probably due to an unsupported record type")
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rrs []dns.RR
	for rows.Next() {
		rr, err := scan(rows)
		if err != nil {
			return nil, err
		}

		rrs = append(rrs, rr)
	}

	return rrs, nil
}
