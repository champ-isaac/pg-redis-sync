// Package pgsync is the public entry point of pg-redis-sync.
// For now it holds configuration loading, connection-string handling and
// read-source selection. See source.go for how topologies are used.
package pgsync

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Topology says how many PostgreSQL instances take part.
type Topology string

const (
	// TopologySingle is one PostgreSQL instance. It is the default, and the
	// only behaviour that existed before topologies were introduced.
	TopologySingle Topology = "single"
	// TopologyPrimaryReplica is one main (primary) instance plus one or more
	// read replicas of it. The main instance is always the CDC source.
	TopologyPrimaryReplica Topology = "primary_replica"
)

// ReadFallback says what to do when no replica is usable for a read.
type ReadFallback string

const (
	// FallbackPrimary reads from the main instance instead. The default.
	FallbackPrimary ReadFallback = "primary"
	// FallbackNone fails the read instead of loading the main instance.
	FallbackNone ReadFallback = "none"
)

const (
	DefaultSlot        = "pg_redis_sync"
	DefaultPublication = "pg_redis_sync_pub"
	// DefaultMaxReplicaLagBytes is how far (in WAL bytes) a replica may trail
	// the main instance and still be used for reads.
	DefaultMaxReplicaLagBytes int64 = 64 << 20
)

// Slot names are restricted by PostgreSQL. Publication names end up inside the
// START_REPLICATION options string, so they are limited to plain identifiers.
var (
	slotRE        = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	publicationRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
)

type Postgres struct {
	// Topology is single (also the meaning of "") or primary_replica.
	Topology Topology
	// DSN is the main (primary) instance, as a postgres:// URL without the
	// replication parameter. It is the authoritative source: the slot, the
	// publication and the CDC stream always live here, in every topology.
	DSN string
	// ReplicaDSNs are read replicas of the main instance, in order of
	// preference. Only allowed with primary_replica; they must point at other
	// instances and the same database as DSN.
	ReplicaDSNs []string
	Slot        string
	Publication string
	// MaxReplicaLagBytes is the largest replica lag still accepted for reads.
	// Must be > 0 with primary_replica; ignored with single.
	MaxReplicaLagBytes int64
	// ReadFallback applies when no replica qualifies. "" means FallbackPrimary.
	ReadFallback ReadFallback
}

type Redis struct {
	Addr     string // host:port
	Password string
	DB       int
}

type Config struct {
	Postgres Postgres
	Redis    Redis
}

// LoadFromEnv reads PG_TOPOLOGY, PG_DSN, PG_REPLICA_DSNS (comma separated),
// PG_REPLICA_MAX_LAG_BYTES, PG_READ_FALLBACK, PG_SLOT, PG_PUBLICATION,
// REDIS_ADDR, REDIS_PASSWORD and REDIS_DB, applies defaults, and validates the
// result. With only PG_DSN and REDIS_ADDR set it behaves as a single instance.
// Pass os.Getenv in production and a map lookup in tests.
func LoadFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{
		Postgres: Postgres{
			Topology:           Topology(orDefault(getenv("PG_TOPOLOGY"), string(TopologySingle))),
			DSN:                getenv("PG_DSN"),
			ReplicaDSNs:        splitList(getenv("PG_REPLICA_DSNS")),
			Slot:               orDefault(getenv("PG_SLOT"), DefaultSlot),
			Publication:        orDefault(getenv("PG_PUBLICATION"), DefaultPublication),
			MaxReplicaLagBytes: DefaultMaxReplicaLagBytes,
			ReadFallback:       ReadFallback(orDefault(getenv("PG_READ_FALLBACK"), string(FallbackPrimary))),
		},
		Redis: Redis{
			Addr:     getenv("REDIS_ADDR"),
			Password: getenv("REDIS_PASSWORD"),
		},
	}
	if v := getenv("PG_REPLICA_MAX_LAG_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("PG_REPLICA_MAX_LAG_BYTES %q is not an integer", v)
		}
		cfg.Postgres.MaxReplicaLagBytes = n
	}
	if v := getenv("REDIS_DB"); v != "" {
		db, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("REDIS_DB %q is not an integer", v)
		}
		cfg.Redis.DB = db
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate reports every problem at once. Errors never contain a DSN,
// because it carries the password.
func (c Config) Validate() error {
	errs := c.Postgres.validate()
	if host, port, err := net.SplitHostPort(c.Redis.Addr); err != nil || host == "" || port == "" {
		errs = append(errs, errors.New("redis.addr must be host:port"))
	}
	if c.Redis.DB < 0 {
		errs = append(errs, errors.New("redis.db must be >= 0"))
	}
	return errors.Join(errs...)
}

func (p Postgres) topology() Topology {
	if p.Topology == "" {
		return TopologySingle
	}
	return p.Topology
}

func (p Postgres) validate() []error {
	var errs []error
	if err := checkDSN(p.DSN); err != nil {
		errs = append(errs, err)
	}
	if !slotRE.MatchString(p.Slot) {
		errs = append(errs, errors.New("postgres.slot must be 1-63 characters of a-z, 0-9 or _"))
	}
	if !publicationRE.MatchString(p.Publication) {
		errs = append(errs, errors.New("postgres.publication must be an identifier (letters, digits, _; max 63)"))
	}
	switch p.ReadFallback {
	case "", FallbackPrimary, FallbackNone:
	default:
		errs = append(errs, errors.New("postgres.read_fallback must be primary or none"))
	}
	switch p.topology() {
	case TopologySingle:
		if len(p.ReplicaDSNs) > 0 {
			errs = append(errs, errors.New("postgres.replicas is only allowed with topology primary_replica"))
		}
	case TopologyPrimaryReplica:
		errs = append(errs, p.validateReplicas()...)
	default:
		errs = append(errs, errors.New("postgres.topology must be single or primary_replica"))
	}
	return errs
}

func (p Postgres) validateReplicas() []error {
	var errs []error
	if len(p.ReplicaDSNs) == 0 {
		errs = append(errs, errors.New("topology primary_replica needs at least one entry in postgres.replicas"))
	}
	if p.MaxReplicaLagBytes <= 0 {
		errs = append(errs, errors.New("postgres.replica_max_lag_bytes must be > 0"))
	}
	// If the main DSN is invalid that is already reported; skip comparisons.
	main, mainErr := parsePGURL(p.DSN)
	seen := map[string]bool{}
	for i, dsn := range p.ReplicaDSNs {
		u, err := parsePGURL(dsn)
		if err != nil {
			errs = append(errs, fmt.Errorf("postgres.replicas[%d] must be a URL like postgres://user:pass@host:5432/dbname", i))
			continue
		}
		ep := endpoint(u)
		if seen[ep] {
			errs = append(errs, fmt.Errorf("postgres.replicas[%d] duplicates another replica", i))
		}
		seen[ep] = true
		if mainErr != nil {
			continue
		}
		if ep == endpoint(main) {
			errs = append(errs, fmt.Errorf("postgres.replicas[%d] points at the main instance", i))
		}
		if u.Path != main.Path {
			errs = append(errs, fmt.Errorf("postgres.replicas[%d] uses a different database than the main instance", i))
		}
	}
	return errs
}

// ReplicationDSN returns dsn with replication=database set, which PostgreSQL
// needs for a logical replication connection. Any existing value is replaced.
func ReplicationDSN(dsn string) (string, error) {
	u, err := parsePGURL(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// RedactDSN hides the password so the DSN is safe to log.
func RedactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid dsn>"
	}
	return u.Redacted()
}

func checkDSN(dsn string) error {
	if dsn == "" {
		return errors.New("postgres.dsn is required")
	}
	_, err := parsePGURL(dsn)
	return err
}

// parsePGURL deliberately drops the url.Parse error: it quotes the input,
// and the input contains the password.
func parsePGURL(dsn string) (*url.URL, error) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") ||
		u.Host == "" || u.Path == "" || u.Path == "/" {
		return nil, errors.New("postgres.dsn must be a URL like postgres://user:pass@host:5432/dbname")
	}
	return u, nil
}

// endpoint is the normalised host:port of a DSN, used to tell instances apart.
func endpoint(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	return net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
