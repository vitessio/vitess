package reparentutil

import "github.com/slackhq/vitess-addons/go/durability"

func init() {
	RegisterDurability("slack_cross_cell", func() Durabler {
		return &durability.SlackCrossCell{}
	})
}
