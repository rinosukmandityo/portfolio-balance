package portfolio

import (
	"context"
)

type (
	// processIDContextKey is a special type used as a key for accessing process ID in a context.
	processIDContextKey struct{}
)

// AddProcessID adds new process ID and put it into context.
func AddProcessID(ctx context.Context, processID string) context.Context {
	return context.WithValue(ctx, processIDContextKey{}, processID)
}

// GetProcessID gets process ID from context.
func GetProcessID(ctx context.Context) string {
	val, ok := ctx.Value(processIDContextKey{}).(string)
	if !ok {
		return ""
	}

	return val
}
