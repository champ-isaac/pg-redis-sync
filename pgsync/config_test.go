package pgsync

import (
	"strings"
	"testing"
)

const goodDSN = "postgres://sync:s3cret@localhost:5432/pgredissync?sslmode=disable"

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadFromEnv(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cfg, err := LoadFromEnv(env(map[string]string{"PG_DSN": goodDSN, "REDIS_ADDR": "localhost:6379"}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Postgres.Slot != DefaultSlot || cfg.Postgres.Publication != DefaultPublication {
			t.Errorf("defaults not applied: %+v", cfg.Postgres)
		}
		if cfg.Redis.DB != 0 || cfg.Redis.Password != "" {
			t.Errorf("unexpected redis config: %+v", cfg.Redis)
		}
	})

	t.Run("all values", func(t *testing.T) {
		cfg, err := LoadFromEnv(env(map[string]string{
			"PG_DSN": goodDSN, "PG_SLOT": "my_slot", "PG_PUBLICATION": "my_pub",
			"REDIS_ADDR": "redis:6379", "REDIS_PASSWORD": "pw", "REDIS_DB": "3",
		}))
		if err != nil {
			t.Fatal(err)
		}
		want := Config{
			Postgres: Postgres{DSN: goodDSN, Slot: "my_slot", Publication: "my_pub"},
			Redis:    Redis{Addr: "redis:6379", Password: "pw", DB: 3},
		}
		if cfg != want {
			t.Errorf("got %+v, want %+v", cfg, want)
		}
	})

	t.Run("REDIS_DB not a number", func(t *testing.T) {
		_, err := LoadFromEnv(env(map[string]string{"PG_DSN": goodDSN, "REDIS_ADDR": "r:1", "REDIS_DB": "x"}))
		if err == nil || !strings.Contains(err.Error(), "REDIS_DB") {
			t.Errorf("want REDIS_DB error, got %v", err)
		}
	})

	t.Run("missing required", func(t *testing.T) {
		_, err := LoadFromEnv(env(nil))
		if err == nil {
			t.Fatal("want error")
		}
		for _, want := range []string{"postgres.dsn is required", "redis.addr"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})
}

func TestValidate(t *testing.T) {
	ok := Config{
		Postgres: Postgres{DSN: goodDSN, Slot: "slot_1", Publication: "Pub_1"},
		Redis:    Redis{Addr: "localhost:6379"},
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"dsn wrong scheme", func(c *Config) { c.Postgres.DSN = "mysql://u:p@h:1/db" }, "postgres.dsn must be a URL"},
		{"dsn no host", func(c *Config) { c.Postgres.DSN = "postgres:///db" }, "postgres.dsn must be a URL"},
		{"dsn no database", func(c *Config) { c.Postgres.DSN = "postgres://u:p@h:5432/" }, "postgres.dsn must be a URL"},
		{"dsn unparsable", func(c *Config) { c.Postgres.DSN = "postgres://u:p@h:bad/db" }, "postgres.dsn must be a URL"},
		{"slot uppercase", func(c *Config) { c.Postgres.Slot = "Slot" }, "postgres.slot"},
		{"slot too long", func(c *Config) { c.Postgres.Slot = strings.Repeat("a", 64) }, "postgres.slot"},
		{"publication injection", func(c *Config) { c.Postgres.Publication = "p', x '1" }, "postgres.publication"},
		{"publication leading digit", func(c *Config) { c.Postgres.Publication = "1pub" }, "postgres.publication"},
		{"redis no port", func(c *Config) { c.Redis.Addr = "localhost" }, "redis.addr"},
		{"redis no host", func(c *Config) { c.Redis.Addr = ":6379" }, "redis.addr"},
		{"redis empty port", func(c *Config) { c.Redis.Addr = "localhost:" }, "redis.addr"},
		{"redis negative db", func(c *Config) { c.Redis.DB = -1 }, "redis.db"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ok
			tt.mutate(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("want error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestValidateErrorsDoNotLeakPassword(t *testing.T) {
	// The port is invalid, so url.Parse fails and would normally quote the input.
	c := Config{
		Postgres: Postgres{DSN: "postgres://sync:s3cret@h:bad/db", Slot: "s", Publication: "p"},
		Redis:    Redis{Addr: "r:1"},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error leaks the password: %v", err)
	}
}

func TestReplicationDSN(t *testing.T) {
	tests := []struct {
		name, in string
		want     []string // substrings that must appear
	}{
		{"adds parameter", goodDSN, []string{"replication=database", "sslmode=disable", "sync:s3cret@localhost:5432/pgredissync"}},
		{"no query", "postgres://u:p@h:5432/db", []string{"?replication=database"}},
		{"replaces existing", "postgres://u:p@h:5432/db?replication=false", []string{"replication=database"}},
		{"postgresql scheme", "postgresql://u:p@h:5432/db", []string{"postgresql://", "replication=database"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReplicationDSN(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("%q does not contain %q", got, w)
				}
			}
			if strings.Contains(got, "replication=false") {
				t.Errorf("old value kept: %q", got)
			}
		})
	}

	if _, err := ReplicationDSN("not a url"); err == nil {
		t.Error("want error for invalid dsn")
	}
}

func TestRedactDSN(t *testing.T) {
	if got := RedactDSN(goodDSN); strings.Contains(got, "s3cret") || !strings.Contains(got, "sync:xxxxx@localhost") {
		t.Errorf("password not redacted: %q", got)
	}
	if got := RedactDSN("postgres://h/db"); got != "postgres://h/db" {
		t.Errorf("dsn without password changed: %q", got)
	}
	if got := RedactDSN("postgres://u:s3cret@h:bad/db"); got != "<invalid dsn>" {
		t.Errorf("want placeholder for unparsable dsn, got %q", got)
	}
}
