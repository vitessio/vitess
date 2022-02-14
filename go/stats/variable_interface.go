package stats

// Variable is the minimal interface which each type in this "stats" package
// must implement.
// When integrating the Vitess stats types ("variables") with the different
// monitoring systems, you can rely on this interface.
type Variable interface {
	// Help returns the description of the variable.
	Help() string

	// String must implement String() from the expvar.Var interface.
	String() string
}
