package charset

func iteratorUTF32(p []byte) (rune, int) {
	if len(p) < 4 {
		return RuneError, 0
	}
	return (rune(p[0]) << 24) | (rune(p[1]) << 16) | (rune(p[2]) << 8) | rune(p[3]), 4
}
