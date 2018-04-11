package database

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/alecthomas/template"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/mholt/caddy"
	"github.com/miekg/dns"
)

var (
	defaultQueries = map[uint16]string{
		dns.TypeA:     `SELECT name, ttl, addr FROM a_record WHERE name = '{{.Name}}'`,
		dns.TypeAAAA:  `SELECT name, ttl, addr FROM aaaa_record WHERE name = '{{.Name}}'`,
		dns.TypeCNAME: `SELECT name, ttl, target FROM cname_record WHERE name = '{{.Name}}'`,
	}
)

func init() {
	caddy.RegisterPlugin("database", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	d := &Database{}

	dc, err := databaseParse(c)
	if err != nil {
		return plugin.Error("database", err)
	}
	d.Config = dc

	queryer, err := newDatabaseQueryer(d.Config.URL, d.Config.TLSArgs...)
	if err != nil {
		return err
	}
	d.Queryer = queryer

	templateQueries := make(map[uint16]*template.Template)
	for k, v := range dc.Queries {
		tv, err := template.New("query").Parse(v)
		if err != nil {
			return err
		}
		templateQueries[k] = tv
	}
	d.Queries = templateQueries

	c.OnStartup(func() error {
		return nil
	})

	c.OnShutdown(func() error {
		return nil
	})

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		d.Next = next
		return d
	})

	return nil
}

func databaseParse(c *caddy.Controller) (*DatabaseConfig, error) {
	dc := &DatabaseConfig{}

	var (
		err     error
		queries = defaultQueries
	)
	for c.Next() {
		dc.Zones = c.RemainingArgs()
		if len(dc.Zones) == 0 {
			dc.Zones = make([]string, len(c.ServerBlockKeys))
			copy(dc.Zones, c.ServerBlockKeys)
		}

		for i, str := range dc.Zones {
			dc.Zones[i] = plugin.Host(str).Normalize()
		}

		if c.NextBlock() {
			for {
				val := c.Val()
				switch val {
				case "url":
					if !c.NextArg() {
						return nil, c.ArgErr()
					}
					dc.URL, err = url.Parse(c.Val())
					if err != nil {
						return nil, err
					}
				case "tls": // cert key cacertfile
					dc.TLSArgs = c.RemainingArgs()
				default:
					i := strings.LastIndex(val, "_query")
					if i != -1 {
						if !c.NextArg() {
							return nil, c.ArgErr()
						}

						var t uint16
						switch val[:i] {
						case "a":
							t = dns.TypeA
						case "aaaa":
							t = dns.TypeAAAA
						case "cname":
							t = dns.TypeCNAME
						default:
							return nil, fmt.Errorf("unsupported type: %s", val[:i])
						}

						query := c.Val()
						if c.NextArg() {
							return nil, fmt.Errorf("unable to parse, make sure %s is properly quoted", val)
						}

						queries[t] = query
					}
				}

				if !c.Next() {
					break
				}
			}
		}
	}

	if dc.URL == nil || dc.URL.Scheme == "" {
		return nil, errors.New("url required")
	}

	dc.Queries = queries

	return dc, nil
}
