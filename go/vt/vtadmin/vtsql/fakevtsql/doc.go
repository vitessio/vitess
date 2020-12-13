// Package fakevtsql provides an interface for mocking out sql.DB responses in
// tests that depend on a vtsql.DB instance.
//
// To use fakevtsql, you will need to create a discovery implementation that
// does not error, e.g. with fakediscovery:
//
//		disco := fakediscovery.New()
//		disco.AddTaggedGates(nil, []*vtadminpb.VTGate{Hostname: "gate"})
//
// Then, you will call vtsql.Parse(), passing the faked discovery implementation:
//
//		vtsqlCfg, err := vtsql.Parse("clusterID", "clusterName", disco, []string{})
//		db := vtsql.New("clusterID", vtsqlCfg)
//
// Finally, with your instantiated VTGateProxy instance, you can mock out the
// DialFunc to always return a fakevtsql.Connector. The Tablets and ShouldErr
// attributes of the connector control the behavior:
//
//		db.DialFunc = func(cfg vitessdriver.Configuration) (*sql.DB, error) {
//			return sql.OpenDB(&fakevtsql.Connector{
//				Tablets: mockTablets,
//				ShouldErr: shouldErr,
//			})
//		}
//		cluster := &cluster.Cluster{
//			/* other attributes */
//			DB: db,
//		}
//
// go/vt/vtadmin/api_test.go has several examples of usage.
package fakevtsql
