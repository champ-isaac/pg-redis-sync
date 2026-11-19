// Package pgsync is the public entry point of pg-redis-sync.
// For now it only holds configuration loading and connection-string handling.
package pgsync

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
)

const (
	DefaultSlot        = "pg_redis_sync"
	DefaultPublication = "pg_redis_sync_pub"
)

// Slot names are restricted by PostgreSQL. Publication names end up inside the
// START_REPLICATION options string, so they are limited to plain identifiers.
var (
	slotRE        = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	publicationRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
)

type Postgres struct {
	DSN         string // postgres:// URL, without the replication parameter
	Slot        string
	Publication string
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

// LoadFromEnv reads PG_DSN, PG_SLOT, PG_PUBLICATION, REDIS_ADDR, REDIS_PASSWORD
// and REDIS_DB, applies defaults, and validates the result.
// Pass os.Getenv in production and a map lookup in tests.
func LoadFromEnv(getenv func(string) string) (Config, error) {
	cfg := Config{
		Postgres: Postgres{
			DSN:         getenv("PG_DSN"),
			Slot:        orDefault(getenv("PG_SLOT"), DefaultSlot),
			Publication: orDefault(getenv("PG_PUBLICATION"), DefaultPublication),
		},
		Redis: Redis{
			Addr:     getenv("REDIS_ADDR"),
			Password: getenv("REDIS_PASSWORD"),
		},
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

// Validate reports every problem at once. Errors never contain the DSN,
// because it carries the password.
func (c Config) Validate() error {
	var errs []error
	if err := checkDSN(c.Postgres.DSN); err != nil {
		errs = append(errs, err)
	}
	if !slotRE.MatchString(c.Postgres.Slot) {
		errs = append(errs, errors.New("postgres.slot must be 1-63 characters of a-z, 0-9 or _"))
	}
	if !publicationRE.MatchString(c.Postgres.Publication) {
		errs = append(errs, errors.New("postgres.publication must be an identifier (letters, digits, _; max 63)"))
	}
	if host, port, err := net.SplitHostPort(c.Redis.Addr); err != nil || host == "" || port == "" {
		errs = append(errs, errors.New("redis.addr must be host:port"))
	}
	if c.Redis.DB < 0 {
		errs = append(errs, errors.New("redis.db must be >= 0"))
	}
	return errors.Join(errs...)
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

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
