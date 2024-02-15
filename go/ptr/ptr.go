package ptr

func Of[T any](x T) *T {
	return &x
}

func Unwrap[T any](x *T, default_ T) T {
	if x != nil {
		return *x
	}
	return default_
}
