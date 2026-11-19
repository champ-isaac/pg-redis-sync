package pgsync

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeProber answers per DSN and records which DSNs were probed.
type fakeProber struct {
	status map[string]ReplicaStatus
	err    map[string]error
	calls  []string
}

func (f *fakeProber) ProbeReplica(_ context.Context, dsn string) (ReplicaStatus, error) {
	f.calls = append(f.calls, dsn)
	return f.status[dsn], f.err[dsn]
}

func healthy(lag int64) ReplicaStatus { return ReplicaStatus{InRecovery: true, LagBytes: lag} }

func replicaConfig(fallback ReadFallback) Postgres {
	return Postgres{
		Topology: TopologyPrimaryReplica, DSN: goodDSN, ReplicaDSNs: []string{replicaDSN, replica2DSN},
		Slot: "s", Publication: "p", MaxReplicaLagBytes: 100, ReadFallback: fallback,
	}
}

func TestStreamDSNIsAlwaysTheMainInstance(t *testing.T) {
	for name, p := range map[string]Postgres{
		"single":          {DSN: goodDSN},
		"primary_replica": replicaConfig(""),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := p.StreamDSN()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got, "localhost:5432") || !strings.Contains(got, "replication=database") {
				t.Errorf("stream DSN is not the main instance with replication=database: %q", got)
			}
			if strings.Contains(got, "replica1") || strings.Contains(got, "replica2") {
				t.Errorf("stream DSN points at a replica: %q", got)
			}
		})
	}
	if _, err := (Postgres{DSN: "nope"}).StreamDSN(); err == nil {
		t.Error("want error for invalid dsn")
	}
}

func TestSelectReadSourceSingle(t *testing.T) {
	// A nil prober proves single never probes.
	got, err := Postgres{DSN: goodDSN}.SelectReadSource(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.DSN != goodDSN || got.Role != RolePrimary || len(got.Skipped) != 0 {
		t.Errorf("unexpected source: %+v", got)
	}
	got, err = Postgres{Topology: TopologySingle, DSN: goodDSN}.SelectReadSource(context.Background(), nil)
	if err != nil || got.Role != RolePrimary {
		t.Errorf("explicit single: %+v, %v", got, err)
	}
}

func TestSelectReadSourcePrimaryReplica(t *testing.T) {
	boom := errors.New("connection refused")

	tests := []struct {
		name        string
		fallback    ReadFallback
		prober      *fakeProber
		wantDSN     string
		wantRole    Role
		wantSkipped []string // substrings, in order
		wantProbes  int
		wantErrIs   error
	}{
		{
			name:       "first replica is used when healthy",
			prober:     &fakeProber{status: map[string]ReplicaStatus{replicaDSN: healthy(0), replica2DSN: healthy(0)}},
			wantDSN:    replicaDSN,
			wantRole:   RoleReplica,
			wantProbes: 1,
		},
		{
			name:        "unreachable first replica is skipped",
			prober:      &fakeProber{status: map[string]ReplicaStatus{replica2DSN: healthy(5)}, err: map[string]error{replicaDSN: boom}},
			wantDSN:     replica2DSN,
			wantRole:    RoleReplica,
			wantSkipped: []string{"replica[0]", "probe failed: connection refused"},
			wantProbes:  2,
		},
		{
			name: "lag exactly at the limit is accepted",
			prober: &fakeProber{status: map[string]ReplicaStatus{
				replicaDSN: healthy(100),
			}},
			wantDSN:    replicaDSN,
			wantRole:   RoleReplica,
			wantProbes: 1,
		},
		{
			name: "lagging and promoted replicas are skipped",
			prober: &fakeProber{status: map[string]ReplicaStatus{
				replicaDSN:  healthy(101),
				replica2DSN: {InRecovery: false},
			}},
			wantDSN:     goodDSN,
			wantRole:    RolePrimary, // default fallback
			wantSkipped: []string{"replica[0]", "lag 101 bytes exceeds limit 100", "replica[1]", "not in recovery"},
			wantProbes:  2,
		},
		{
			name:        "fallback none fails when nothing qualifies",
			fallback:    FallbackNone,
			prober:      &fakeProber{err: map[string]error{replicaDSN: boom, replica2DSN: boom}},
			wantErrIs:   ErrNoReadSource,
			wantSkipped: []string{"replica[0]", "replica[1]"},
			wantProbes:  2,
		},
		{
			name:       "explicit fallback primary",
			fallback:   FallbackPrimary,
			prober:     &fakeProber{err: map[string]error{replicaDSN: boom, replica2DSN: boom}},
			wantDSN:    goodDSN,
			wantRole:   RolePrimary,
			wantProbes: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := replicaConfig(tt.fallback).SelectReadSource(context.Background(), tt.prober)
			if len(tt.prober.calls) != tt.wantProbes {
				t.Errorf("probed %d replicas, want %d", len(tt.prober.calls), tt.wantProbes)
			}
			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("want %v, got %v", tt.wantErrIs, err)
				}
				assertContainsInOrder(t, err.Error(), tt.wantSkipped)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.DSN != tt.wantDSN || got.Role != tt.wantRole {
				t.Errorf("got %s (%s), want %s (%s)", got.DSN, got.Role, tt.wantDSN, tt.wantRole)
			}
			assertContainsInOrder(t, strings.Join(got.Skipped, "\n"), tt.wantSkipped)
			for _, s := range got.Skipped {
				if strings.Contains(s, "s3cret") {
					t.Errorf("skip reason leaks the password: %q", s)
				}
			}
		})
	}
}

func TestSelectReadSourceErrors(t *testing.T) {
	if _, err := replicaConfig("").SelectReadSource(context.Background(), nil); err == nil {
		t.Error("want error for a nil prober with primary_replica")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pr := &fakeProber{}
	if _, err := replicaConfig("").SelectReadSource(ctx, pr); !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", err)
	}
	if len(pr.calls) != 0 {
		t.Errorf("probed after cancellation: %v", pr.calls)
	}
}

func assertContainsInOrder(t *testing.T, s string, parts []string) {
	t.Helper()
	rest := s
	for _, p := range parts {
		i := strings.Index(rest, p)
		if i < 0 {
			t.Errorf("%q not found in order in %q", p, s)
			return
		}
		rest = rest[i+len(p):]
	}
}
