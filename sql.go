package sql

import (
	"bytes"
	"fmt"
	"net/url"

	"github.com/alecthomas/template"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/dnsutil"
	"github.com/coredns/coredns/request"
	"github.com/glerchundi/coredns-sql/query"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

const (
	name = "sql"
)

// SQL is the plugin handler
type SQL struct {
	Next    plugin.Handler
	Queryer query.Queryer
	Queries map[uint16]*template.Template

	Config *SQLConfig
}

type SQLConfig struct {
	Zones   []string
	URL     *url.URL
	TLSArgs []string
	Queries map[uint16]string
}

// ServeDNS implements the plugin.Handle interface.
func (d *SQL) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r, Context: ctx}

	zone := plugin.Zones(d.Config.Zones).Matches(state.Name())
	if zone == "" {
		return plugin.NextOrFailure(d.Name(), d.Next, ctx, w, r)
	}

	qtype := state.QType()
	queryTemplate, ok := d.Queries[qtype]
	if !ok {
		return plugin.NextOrFailure(d.Name(), d.Next, ctx, w, r)
	}

	var queryRendered bytes.Buffer
	err := queryTemplate.Execute(&queryRendered, struct {
		Name string
		Type uint16
	}{Name: state.QName(), Type: qtype})
	if err != nil {
		return dns.RcodeServerFailure, plugin.Error(d.Name(), err)
	}

	records, err := d.Queryer.Query(queryRendered.String(), query.Scan(qtype))
	if err != nil {
		return dns.RcodeServerFailure, plugin.Error(d.Name(), err)
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, true, true
	m.Answer = append(m.Answer, records...)

	m = dnsutil.Dedup(m)
	state.SizeAndDo(m)
	m, _ = state.Scrub(m)
	w.WriteMsg(m)
	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (d *SQL) Name() string { return name }

func newDatabaseQueryer(u *url.URL, tlsArgs ...string) (query.Queryer, error) {
	switch u.Scheme {
	case "postgres", "postgresql":
		return query.NewPostgresQueryer(u, tlsArgs...)
	case "mysql":
		return query.NewMySQLQueryer(u, tlsArgs...)
	default:
		return nil, fmt.Errorf("unsupported scheme: '%s'", u.Scheme)
	}
}
