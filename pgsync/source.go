package pgsync

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Source selection rules, the same in both topologies:
//
//   - The CDC stream (slot, publication, START_REPLICATION) always uses the
//     main instance. StreamDSN is the only way to get that DSN.
//   - Read-only queries (backfill reads, reconciliation, lag checks) may use a
//     replica. SelectReadSource picks one.

// Role is what a server is, relative to the main instance.
type Role string

const (
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

// ErrNoReadSource means no replica qualified and fallback to the main instance
// is switched off.
var ErrNoReadSource = errors.New("no usable read source")

// ReplicaStatus is what a probe learned about one replica.
type ReplicaStatus struct {
	// InRecovery is false when the server is not a standby: a misconfigured
	// DSN, or a replica that was promoted. Such a server is never used.
	InRecovery bool
	// LagBytes is the main instance's WAL position minus the replica's replay
	// position.
	LagBytes int64
}

// Prober inspects a replica. The pgx-based implementation comes with the
// replication consumer (milestone 1); tests use a fake.
type Prober interface {
	ProbeReplica(ctx context.Context, dsn string) (ReplicaStatus, error)
}

// ReadSource is the server chosen for read-only queries.
type ReadSource struct {
	DSN  string
	Role Role
	// Skipped lists replicas that were tried and rejected, with the reason.
	// Passwords are redacted. Log it when Role is primary in a primary_replica setup.
	Skipped []string
}

// StreamDSN returns the DSN for the logical replication connection:
// always the main instance, with replication=database added.
func (p Postgres) StreamDSN() (string, error) {
	return ReplicationDSN(p.DSN)
}

// SelectReadSource picks the server for read-only queries. It assumes the
// config passed Validate.
//
// single: the one instance, no probing.
// primary_replica: the first replica, in configured order, that is reachable,
// is in recovery, and lags by at most MaxReplicaLagBytes. If none qualifies it
// returns the main instance, or ErrNoReadSource when ReadFallback is "none".
func (p Postgres) SelectReadSource(ctx context.Context, pr Prober) (ReadSource, error) {
	if p.topology() == TopologySingle {
		return ReadSource{DSN: p.DSN, Role: RolePrimary}, nil
	}
	if pr == nil {
		return ReadSource{}, errors.New("a Prober is required for topology primary_replica")
	}

	var skipped []string
	for i, dsn := range p.ReplicaDSNs {
		if err := ctx.Err(); err != nil {
			return ReadSource{}, err
		}
		st, err := pr.ProbeReplica(ctx, dsn)
		reason := ""
		switch {
		case err != nil:
			reason = "probe failed: " + err.Error()
		case !st.InRecovery:
			reason = "not in recovery (not a standby)"
		case st.LagBytes > p.MaxReplicaLagBytes:
			reason = fmt.Sprintf("lag %d bytes exceeds limit %d", st.LagBytes, p.MaxReplicaLagBytes)
		}
		if reason == "" {
			return ReadSource{DSN: dsn, Role: RoleReplica, Skipped: skipped}, nil
		}
		skipped = append(skipped, fmt.Sprintf("replica[%d] %s: %s", i, RedactDSN(dsn), reason))
	}

	if p.ReadFallback == FallbackNone {
		return ReadSource{}, fmt.Errorf("%w: %s", ErrNoReadSource, strings.Join(skipped, "; "))
	}
	return ReadSource{DSN: p.DSN, Role: RolePrimary, Skipped: skipped}, nil
}
