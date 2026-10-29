package slogdirect

import (
	"context"
	"fmt"
	"log/slog"
)

func calls(ctx context.Context, name string, l *slog.Logger) {
	slog.Info(fmt.Sprintf("bad %s", name)) // want `log through go/vt/log, not log/slog directly`
	slog.Info("constant")                  // want `log through go/vt/log, not log/slog directly`
	slog.Default().Error(name)             // want `log through go/vt/log, not log/slog directly`
	slog.Log(ctx, slog.LevelWarn, name)    // want `log through go/vt/log, not log/slog directly`
	slog.DebugContext(ctx, name)           // want `log through go/vt/log, not log/slog directly`
	l.Warn(name)                           // want `log through go/vt/log, not log/slog directly`
	l.LogAttrs(ctx, slog.LevelInfo, name)  // want `log through go/vt/log, not log/slog directly`
	(*slog.Logger).Info(l, name)           // want `log through go/vt/log, not log/slog directly`

	f := slog.Error // want `log through go/vt/log, not log/slog directly`
	f(name)
	g := l.Info // want `log through go/vt/log, not log/slog directly`
	g(name)
}

func notLogging(ctx context.Context, name string, l *slog.Logger) {
	_ = slog.String("key", name)
	_ = l.With(slog.String("key", name))
	_ = l.WithGroup(name)
	_ = l.Enabled(ctx, slog.LevelInfo)
	_ = slog.New(l.Handler())
	_ = slog.Default()
}
