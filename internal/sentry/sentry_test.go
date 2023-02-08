package sentry

import (
	"testing"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/require"
)

func TestStripSensitiveData_Success(t *testing.T) {
	request := &sentry.Request{
		Data:    "data",
		Cookies: "cookies",
		Headers: map[string]string{
			"X-Test": "header",
		},
	}

	event := &sentry.Event{
		Request: request,
	}
	StripSensitiveData(event, nil)

	require.Empty(t, request.Data)
	require.Empty(t, request.Cookies)
	require.Empty(t, request.Headers)
}
