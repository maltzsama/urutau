// Package worker implements the worker plane: per-table batchers that
// accumulate changes and flush them collapsed to a committer, with commits
// strictly serialized per table. For now the process runs
// collapsed — changes arrive over an in-process channel; the gRPC and Arrow
// Flight planes arrive with multi-worker support.
package worker
