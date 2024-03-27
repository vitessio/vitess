package boost

type Columns map[string]string

type PlanConfig struct {
	IsBoosted    bool
	BoostColumns Columns
}

func NonBoostedPlanConfig() *PlanConfig {
	return &PlanConfig{
		IsBoosted:    false,
		BoostColumns: Columns{},
	}
}

type QueryFilterConfig struct {
	Columns   []string `yaml:"columns"`
	TableName string   `yaml:"tableName"`
}

type QueryFilterConfigs struct {
	BoostConfigs []QueryFilterConfig `yaml:"boostConfigs"`
}
