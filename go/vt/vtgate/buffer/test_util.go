package buffer

// SetBufferingModeInTestingEnv should only be used from testing code to change the flag (enable_buffer) default value
func SetBufferingModeInTestingEnv(enabled bool) {
	*bufferEnabled = enabled
}
