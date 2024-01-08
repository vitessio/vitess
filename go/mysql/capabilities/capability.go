package capabilities

type FlavorCapability int

const (
	NoneFlavorCapability                        FlavorCapability = iota // default placeholder
	FastDropTableFlavorCapability                                       // supported in MySQL 8.0.23 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-23.html
	TransactionalGtidExecutedFlavorCapability                           //
	InstantDDLFlavorCapability                                          // ALGORITHM=INSTANT general support
	InstantAddLastColumnFlavorCapability                                //
	InstantAddDropVirtualColumnFlavorCapability                         //
	InstantAddDropColumnFlavorCapability                                // Adding/dropping column in any position/ordinal.
	InstantChangeColumnDefaultFlavorCapability                          //
	InstantExpandEnumCapability                                         //
	MySQLJSONFlavorCapability                                           // JSON type supported
	MySQLUpgradeInServerFlavorCapability                                //
	DynamicRedoLogCapacityFlavorCapability                              // supported in MySQL 8.0.30 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-30.html
	DisableRedoLogFlavorCapability                                      // supported in MySQL 8.0.21 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-21.html
	CheckConstraintsCapability                                          // supported in MySQL 8.0.16 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-16.html
	PerformanceSchemaDataLocksTableCapability
)

type CapableOf func(capability FlavorCapability) (bool, error)
