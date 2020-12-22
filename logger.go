package gmysql

import (
	"github.com/sgs921107/glogging"
)

type (
	// LogFields logrus fields
	LogFields = glogging.Fields
)

var (
	// Logging logging
	Logging = glogging.NewLogging(&glogging.Options{})
)