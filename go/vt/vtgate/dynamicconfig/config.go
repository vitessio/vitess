package dynamicconfig

type DDL interface {
	OnlineEnabled() bool
	DirectEnabled() bool
}
