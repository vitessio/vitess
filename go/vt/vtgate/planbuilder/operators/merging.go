package operators

import (
	"reflect"

	"vitess.io/vitess/go/test/dbg"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

// TableRouting:       EqualUnique, Equal, In, MultiEqual, Scatter, SubShard
// InfoSchemaRouting:  DBA
// RefRouting:         Reference
// Dual:               Reference
// NoneRouting:        None
// ByDestRouting:      ByDestination
// Unsharded:          Unsharded

// Merge checks whether two operators can be merged into a single one.
// If they can be merged, the new Routing for the merged operator is returned.
// If they cannot be merged, nil is returned.
func Merge(op1, op2 ops.Operator) Routing {
	aRoute, bRoute := operatorsToRoutes(op1, op2)
	if aRoute == nil || bRoute == nil {
		return nil
	}

	if getTypeName(aRoute.Routing) < getTypeName(bRoute.Routing) {
		// merging is commutative, and we only want to have to think about one of the two orders, so we do this
		aRoute, bRoute = bRoute, aRoute
	}

	// sameKeyspace := aRoute.Keyspace == bRoute.Keyspace
	//
	// if !sameKeyspace {
	// 	if altARoute := aRoute.AlternateInKeyspace(bRoute.Keyspace); altARoute != nil {
	// 		aRoute = altARoute
	// 		sameKeyspace = true
	// 	} else if altBRoute := bRoute.AlternateInKeyspace(aRoute.Keyspace); altBRoute != nil {
	// 		bRoute = altBRoute
	// 		sameKeyspace = true
	// 	}
	// }

	a, b := getR(aRoute.Routing), getR(bRoute.Routing)
	switch {
	case a.dual: // if either side is a dual query, we can always merge them together
		return bRoute.Routing
	case b.dual: // if either side is a dual query, we can always merge them together
		return aRoute.Routing
	case a.ref && b.infoSchema:

	default:
		panic(dbg.S(a, b))
	}

	return nil
}

func getTypeName(myvar interface{}) string {
	return reflect.TypeOf(myvar).String()
}

func getR(r Routing) rBools {
	switch r.(type) {
	case *InfoSchemaRouting:
		return rBools{infoSchema: true}
	case *ReferenceRouting:
		return rBools{ref: true}
	case *DualRouting:
		return rBools{dual: true}
	}
	return rBools{}
}

type rBools struct {
	table,
	infoSchema,
	ref,
	none,
	unsharded,
	dual,
	dest bool
}
