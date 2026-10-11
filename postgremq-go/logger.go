package postgremq

// Logger is the interface that wraps the basic Printf logging method.
//
// This interface is compatible with the standard log.Logger type and can be
// used with WithLogger to provide custom logging.
type Logger interface {
	// Printf formats and logs a message. Format follows fmt.Printf conventions.
	Printf(format string, v ...interface{})
}

// NoopLogger implements both Logger and LevelLogger interfaces but discards all log output.
//
// This is the default logger used by Connection if no logger is provided via
// WithLogger or WithLevelLogger.
type NoopLogger struct{}

// Infof implements LevelLogger.Infof but does nothing.
func (l NoopLogger) Infof(format string, args ...interface{}) {
}

// Errorf implements LevelLogger.Errorf but does nothing.
func (l NoopLogger) Errorf(format string, args ...interface{}) {
}

// Warnf implements LevelLogger.Warnf but does nothing.
func (l NoopLogger) Warnf(format string, args ...interface{}) {
}

// Debugf implements LevelLogger.Debugf but does nothing.
func (l NoopLogger) Debugf(format string, args ...interface{}) {
}

// Printf implements Logger.Printf but does nothing.
func (l NoopLogger) Printf(format string, v ...interface{}) {}

// LevelLogger interface supports leveled logging with Debug, Info, Warn, and Error methods.
//
// This interface provides more granular control over log output compared to Logger.
// Use WithLevelLogger to provide an implementation.
type LevelLogger interface {
	// Infof logs an informational message.
	Infof(format string, args ...interface{})
	// Errorf logs an error message.
	Errorf(format string, args ...interface{})
	// Warnf logs a warning message.
	Warnf(format string, args ...interface{})
	// Debugf logs a debug message.
	Debugf(format string, args ...interface{})
}

type levelLogAdapter struct {
	logger Logger
}

func (l *levelLogAdapter) Infof(format string, args ...interface{}) {
	l.logger.Printf("INFO: "+format, args...)
}

func (l levelLogAdapter) Errorf(format string, args ...interface{}) {
	l.logger.Printf("ERROR: "+format, args...)
}

func (l levelLogAdapter) Warnf(format string, args ...interface{}) {
	l.logger.Printf("WARN: "+format, args...)
}

func (l levelLogAdapter) Debugf(format string, args ...interface{}) {
	l.logger.Printf("DEBUG: "+format, args...)
}
