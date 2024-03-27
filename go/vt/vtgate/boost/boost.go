package boost

type Columns map[string]string

type PlanConfig struct {
	IsBoosted    bool
	BoostColumns Columns
}
