// Package retry classifies PostgreSQL errors as transient or permanent and
// retries the transient ones with a capped backoff.
package retry
