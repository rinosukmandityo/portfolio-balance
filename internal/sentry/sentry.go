package sentry

import (
	"github.com/getsentry/sentry-go"
)

// StripSensitiveData removes all headers, cookies and body from request before
// sentry submission of transaction event.
func StripSensitiveData(event *sentry.Event, hint *sentry.EventHint) *sentry.Event {
	if event == nil {
		return event
	}

	if event.Request != nil {
		req := event.Request

		// By default we don't want to share headers, cookies and body to sentry.
		// Update this function if you want visibility to specific values.
		// Add sparingly as needed to prevent accidentally sharing of secrets or PII.
		req.Data = ""
		req.Cookies = ""
		req.Headers = map[string]string{}
	}

	return event
}
