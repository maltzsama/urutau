package remote

import "errors"

// ErrCoordinatorLost marks a worker exit caused by losing, or never reaching,
// its coordinator: the network, not the worker. cmd/worker records such an
// exit as "network: " in the Pod's termination message, and the coordinator,
// reading it when the worker comes back, does not count it as a crash
// (issue #461).
var ErrCoordinatorLost = errors.New("coordinator lost")

// coordinatorLost is err marked ErrCoordinatorLost; its message is err's.
type coordinatorLost struct{ err error }

func (e coordinatorLost) Error() string   { return e.err.Error() }
func (e coordinatorLost) Unwrap() []error { return []error{e.err, ErrCoordinatorLost} }

// CoordinatorLost marks err as a loss of the coordinator.
func CoordinatorLost(err error) error { return coordinatorLost{err: err} }
