package server

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	"github.com/osrg/gobgp/v4/pkg/packet/bgp"
)

func lookupTestPath(t *testing.T, family bgp.Family, prefix, nexthop string) *api.Path {
	t.Helper()
	nh, err := bgp.NewPathAttributeNextHop(netip.MustParseAddr(nexthop))
	require.NoError(t, err)
	var nlri bgp.NLRI
	switch family {
	case bgp.RF_IPv4_UC:
		nlri, err = bgp.NewIPAddrPrefix(netip.MustParsePrefix(prefix))
	case bgp.RF_IPv4_MPLS:
		nlri, err = bgp.NewLabeledIPAddrPrefix(netip.MustParsePrefix(prefix), *bgp.NewMPLSLabelStack(100))
	default:
		t.Fatalf("unsupported family %s", family)
	}
	require.NoError(t, err)
	path, err := apiutil.NewPath(family, nlri, false, []bgp.PathAttributeInterface{bgp.NewPathAttributeOrigin(0), nh}, time.Now())
	require.NoError(t, err)
	return path
}

func lookupKey(t *testing.T, family bgp.Family, prefix string) *api.DestinationKey {
	return &api.DestinationKey{
		Family: &api.Family{Afi: api.Family_Afi(family.Afi()), Safi: api.Family_Safi(family.Safi())},
		Nlri:   lookupTestPath(t, family, prefix, "10.0.0.1").Nlri,
	}
}

// Each key is read by its exact destination, labeled unicast included, and
// the answer lists the keys in the order of the request.
func TestLookupDestinations(t *testing.T) {
	s := startContextServer(t)
	g := &server{bgpServer: s, shared: s.shared}
	ctx := context.Background()
	for _, p := range []*api.Path{
		lookupTestPath(t, bgp.RF_IPv4_UC, "10.20.0.0/24", "10.0.0.1"),
		lookupTestPath(t, bgp.RF_IPv4_UC, "10.20.0.0/25", "10.0.0.2"),
		lookupTestPath(t, bgp.RF_IPv4_MPLS, "10.20.0.0/24", "10.0.0.3"),
	} {
		_, err := g.AddPath(ctx, &api.AddPathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: p})
		require.NoError(t, err)
	}
	r, err := g.LookupDestinations(ctx, &api.LookupDestinationsRequest{
		TableType: api.TableType_TABLE_TYPE_GLOBAL,
		Keys: []*api.DestinationKey{
			lookupKey(t, bgp.RF_IPv4_MPLS, "10.20.0.0/24"),
			lookupKey(t, bgp.RF_IPv4_UC, "10.20.0.0/24"),
			lookupKey(t, bgp.RF_IPv4_UC, "10.20.0.0/23"),
			lookupKey(t, bgp.RF_IPv4_UC, "10.20.0.0/25"),
		},
	})
	require.NoError(t, err)
	require.Len(t, r.Destinations, 4)
	nexthops := func(d *api.DestinationLookup) []string {
		var out []string
		for _, p := range d.Paths {
			attrs, err := apiutil.GetNativePathAttributes(p)
			require.NoError(t, err)
			for _, a := range attrs {
				switch a := a.(type) {
				case *bgp.PathAttributeNextHop:
					out = append(out, a.Value.String())
				case *bgp.PathAttributeMpReachNLRI:
					out = append(out, a.Nexthop.String())
				}
			}
		}
		return out
	}
	require.Equal(t, []string{"10.0.0.3"}, nexthops(r.Destinations[0]), "labeled unicast, the label is not part of the key")
	require.Equal(t, []string{"10.0.0.1"}, nexthops(r.Destinations[1]))
	require.Empty(t, r.Destinations[2].Paths, "an exact lookup does not match a covering or covered prefix")
	require.Equal(t, []string{"10.0.0.2"}, nexthops(r.Destinations[3]))
	require.Equal(t, api.Family_SAFI_MPLS_LABEL, r.Destinations[0].Key.Family.Safi)

	// A deleted path is gone at once.
	_, err = g.DeletePath(ctx, &api.DeletePathRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Path: lookupTestPath(t, bgp.RF_IPv4_MPLS, "10.20.0.0/24", "10.0.0.3")})
	require.NoError(t, err)
	r, err = g.LookupDestinations(ctx, &api.LookupDestinationsRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: []*api.DestinationKey{lookupKey(t, bgp.RF_IPv4_MPLS, "10.20.0.0/24")}})
	require.NoError(t, err)
	require.Empty(t, r.Destinations[0].Paths)
}

func TestLookupDestinationsChecks(t *testing.T) {
	s := startContextServer(t)
	g := &server{bgpServer: s, shared: s.shared}
	ctx := context.Background()
	for name, r := range map[string]*api.LookupDestinationsRequest{
		"adj-in":    {TableType: api.TableType_TABLE_TYPE_ADJ_IN},
		"no family": {TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: []*api.DestinationKey{{Nlri: lookupKey(t, bgp.RF_IPv4_UC, "10.0.0.0/8").Nlri}}},
		"no nlri":   {TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: []*api.DestinationKey{{Family: lookupKey(t, bgp.RF_IPv4_UC, "10.0.0.0/8").Family}}},
		"too many":  {TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: make([]*api.DestinationKey, MaxLookupDestinations+1)},
	} {
		_, err := g.LookupDestinations(ctx, r)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%s: %v", name, err)
	}
	r, err := g.LookupDestinations(ctx, &api.LookupDestinationsRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL})
	require.NoError(t, err)
	require.Empty(t, r.Destinations)
}

// On a large table, a lookup of 1,000 keys reads 1,000 destinations, not
// the table.
func TestLookupDestinationsLargeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("large table")
	}
	s := startContextServer(t)
	g := &server{bgpServer: s, shared: s.shared}
	ctx := context.Background()
	const n = 200000
	paths := make([]*api.Path, 0, 10000)
	flush := func() {
		stream := make([]*apiutil.Path, len(paths))
		for i, p := range paths {
			stream[i] = mustApi2apiutilPath(p)
		}
		_, err := s.AddPath(apiutil.AddPathRequest{Paths: stream})
		require.NoError(t, err)
		paths = paths[:0]
	}
	for i := range n {
		a := netip.AddrFrom4([4]byte{20, byte(i >> 16), byte(i >> 8), byte(i)})
		paths = append(paths, lookupTestPath(t, bgp.RF_IPv4_UC, netip.PrefixFrom(a, 32).String(), "10.0.0.1"))
		if len(paths) == cap(paths) {
			flush()
		}
	}
	keys := make([]*api.DestinationKey, 1000)
	for i := range keys {
		a := netip.AddrFrom4([4]byte{20, 0, byte(i >> 8), byte(i)})
		keys[i] = lookupKey(t, bgp.RF_IPv4_UC, netip.PrefixFrom(a, 32).String())
	}
	start := time.Now()
	require.NoError(t, s.mgmtOperation(func() error {
		_ = s.globalRib.GetPathList("global", 0, []bgp.Family{bgp.RF_IPv4_UC})
		return nil
	}, false))
	walk := time.Since(start)
	start = time.Now()
	r, err := g.LookupDestinations(ctx, &api.LookupDestinationsRequest{TableType: api.TableType_TABLE_TYPE_GLOBAL, Keys: keys})
	require.NoError(t, err)
	lookup := time.Since(start)
	t.Logf("%d paths: one walk %v, a lookup of %d keys %v", n, walk, len(keys), lookup)
	for _, d := range r.Destinations {
		require.Len(t, d.Paths, 1)
	}
	require.Less(t, lookup, walk)
}
