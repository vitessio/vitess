/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package config

//
// General-store configuration
//

// StoresSettings is a general settings container for specific stores.
type StoresSettings struct {
	MySQL MySQLConfigurationSettings // Any and all MySQL setups go here

	// Futuristic stores can come here.
}

// Hook to implement adjustments after reading each configuration file.
func (settings *StoresSettings) postReadAdjustments() error {
	if err := settings.MySQL.postReadAdjustments(); err != nil {
		return err
	}
	return nil
}
