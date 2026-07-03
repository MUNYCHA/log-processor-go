package logpkg

import "testing"

// TestNormalizeHighMode pins the structural pattern produced for every token
// category in the default (high) restrict mode. A change in any expectation
// here changes alert fingerprints in production: existing pattern store
// entries stop matching (duplicate alerts get re-sent) or, worse, distinct
// alerts start collapsing into one fingerprint and get suppressed.
func TestNormalizeHighMode(t *testing.T) {
	n := NewLogMessageNormalizer(nil, High, nil)

	cases := []struct {
		name, in, want string
	}{
		{"iso timestamp", "2026-05-19T10:23:45.123Z error", "<TS> error"},
		{"iso timestamp with offset", "2026-05-19 10:23:45+02:00 boot", "<TS> boot"},
		{"apache clf timestamp", "19/May/2026:10:23:45 +0000 GET", "<TS> get"},
		{"haproxy clf timestamp with millis", "[03/Jul/2026:04:12:31.882] req", "[<TS>] req"},
		{"syslog timestamp", "May 19 10:23:45 kernel panic", "<TS> kernel panic"},
		{"bare date", "on 2026-05-19 run", "on <TS> run"},
		{"bare date unpadded", "on 2026-7-3 run", "on <TS> run"},
		{"bare time", "at 10:23:45 done", "at <TS> done"},
		{"time without seconds", "cron at 14:22 fired", "cron at <TS> fired"},
		{"ratio is numbers not a timestamp", "scale 3:1 set", "scale <N>:<N> set"},
		{"url", "fetch https://api.example.com/v1?x=1 failed", "fetch <URL> failed"},
		{"quoted url keeps quote pairing", `upstream: "http://10.0.4.12:8081/v2/checkout", host: "api.example.com"`, "upstream: <STR>, host: <STR>"},
		{"email", "user alice@example.com denied", "user <EMAIL> denied"},
		{"stack frame", "at com.foo.Service.run(Service.java:142)", "at com.foo.service.run(<FILE>:<LINE>)"},
		{"uuid", "id 550e8400-e29b-41d4-a716-446655440000 gone", "id <UUID> gone"},
		{"mac address", "from aa:bb:cc:dd:ee:ff dropped", "from <MAC> dropped"},
		{"0x hex", "addr 0xdeadbeef bad", "addr <HEX> bad"},
		{"bare hex with digits and letters", "token cafebabe1234 expired", "token <HEX> expired"},
		{"all-letter hex kept literal in high", "token deadbeef expired", "token deadbeef expired"},
		{"short mixed hex kept literal in high", "checksum 0a3f bad", "checksum 0a3f bad"},
		{"ipv4 with port", "connect 192.168.1.5:5432 refused", "connect <IP>:<PORT> refused"},
		{"ipv4", "host 192.168.1.5 down", "host <IP> down"},
		{"ipv6", "peer fe80::1ff:fe23:4567:890a lost", "peer <IP6> lost"},
		{"unix path", "open /var/log/app.log failed", "open <PATH> failed"},
		{"single-segment unix path", "mount /data failed", "mount <PATH> failed"},
		{"word-slash-word untouched", "read and/or write I/O", "read and/or write i/o"},
		{"windows path", `read C:\data\logs\app.log failed`, "read <PATH> failed"},
		{"jwt token", "auth denied for eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjoiYm9iIn0.SflKxwRJSMeKK here", "auth denied for <JWT> here"},
		{"semver with prefix and suffix", "plugin v2.14.3-rc1 incompatible", "plugin <VER> incompatible"},
		{"semver plain", "agent 1.8.22 deprecated", "agent <VER> deprecated"},
		{"short git sha", "build a3f8c21 crashed", "build <HEX> crashed"},
		{"size", "used 45GB of quota", "used <SIZE> of quota"},
		{"size binary unit", "limit 2048MiB hit", "limit <SIZE> hit"},
		{"size kernel lowercase unit", "anon-rss:4194304kB used", "anon-rss:<SIZE> used"},
		{"duration seconds", "after 30s timeout", "after <DUR> timeout"},
		{"duration millis", "took 1500ms total", "took <DUR> total"},
		{"compound duration", "timeout after 1h30m elapsed", "timeout after <DUR> elapsed"},
		{"percent", "disk 87% full", "disk <PCT> full"},
		{"bare number", "code 9876 returned", "code <N> returned"},
		{"quoted string", `dropped "primary database" now`, "dropped <STR> now"},
		{"json object", `payload {"a":1,"b":[2,3]} rejected`, "payload <JSON> rejected"},
		{"bracketed array", "shards [1, 2, 3] gone", "shards <ARR> gone"},
		{"key=value plain", "host=db-server down", "host=<VAL> down"},
		{"key=value with placeholder", "pid=4242 exited", "pid=<N> exited"},
		{"lowercasing", "Could NOT Connect", "could not connect"},
		{"identifier-embedded numbers kept in high", "worker7 raised req_123", "worker7 raised req_123"},
		{"empty message", "", ""},
		{
			"readme example",
			"could not connect to 192.168.1.5:5432 after 30s retries=3",
			"could not connect to <IP>:<PORT> after <DUR> retries=<N>",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := n.NormalizeMessage(c.in); got != c.want {
				t.Errorf("NormalizeMessage(%q)\n got:  %q\n want: %q", c.in, got, c.want)
			}
		})
	}
}

// TestNormalizeSameStructureSameFingerprint is the dedup contract: messages
// differing only in variable data must collapse to one pattern, and messages
// with different structure must not.
func TestNormalizeSameStructureSameFingerprint(t *testing.T) {
	n := NewLogMessageNormalizer(nil, High, nil)

	a := n.NormalizeMessage("FATAL: could not connect to 10.0.0.1:5432 after 30s retries=3")
	b := n.NormalizeMessage("FATAL: could not connect to 192.168.7.9:6543 after 45s retries=9")
	if a != b {
		t.Errorf("same structure produced different patterns:\n%q\n%q", a, b)
	}

	c := n.NormalizeMessage("FATAL: disk full on /var/lib/data")
	if a == c {
		t.Errorf("different structure collapsed into one pattern: %q", a)
	}
}

func TestNormalizeMediumMode(t *testing.T) {
	n := NewLogMessageNormalizer(nil, Medium, nil)

	cases := []struct {
		name, in, want string
	}{
		{"identifier-embedded number", "worker7 raised req_123", "worker<N> raised req_<N>"},
		{"all-letter hex", "token deadbeef expired", "token <HEX> expired"},
		{"short mixed hex", "checksum 0a3f bad", "checksum <HEX> bad"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := n.NormalizeMessage(c.in); got != c.want {
				t.Errorf("NormalizeMessage(%q)\n got:  %q\n want: %q", c.in, got, c.want)
			}
		})
	}
}

func TestNormalizeLowMode(t *testing.T) {
	n := NewLogMessageNormalizer(nil, Low, []string{"FATAL"})

	cases := []struct {
		name, in, want string
	}{
		{"words collapse but alert keyword survives", "FATAL disk error worker7", "fatal <TOK> <TOK> worker<N>"},
		{"short words kept", "db up ok", "db up ok"},
		{"key=value token kept", "host=db-server down", "host=<VAL> <TOK>"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := n.NormalizeMessage(c.in); got != c.want {
				t.Errorf("NormalizeMessage(%q)\n got:  %q\n want: %q", c.in, got, c.want)
			}
		})
	}
}

func TestNormalizeCustomRulesRunFirst(t *testing.T) {
	rules := []NormalizerRule{NewNormalizerRule(`worker-\d+`, "<WORKER>")}
	n := NewLogMessageNormalizer(rules, High, nil)

	got := n.NormalizeMessage("worker-42 crashed")
	if want := "<WORKER> crashed"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestParseRestrictMode(t *testing.T) {
	cases := []struct {
		in   string
		want RestrictMode
	}{
		{"low", Low}, {"LOW", Low},
		{"medium", Medium}, {"med", Medium},
		{"high", High}, {" High ", High},
		{"", High}, {"bogus", High},
	}
	for _, c := range cases {
		if got := ParseRestrictMode(c.in); got != c.want {
			t.Errorf("ParseRestrictMode(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
