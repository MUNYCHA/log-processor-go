package logpkg

import (
	"fmt"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
)

// matchTimeout bounds a single regex match. regexp2 is a backtracking engine:
// without a timeout, a pathological line (or a custom rule with nested
// quantifiers) can make one match take effectively forever, pinning the
// partition goroutine's CPU core with no recovery. Normal lines match in
// microseconds and never come near this limit. On timeout NormalizeMessage
// returns an error and the caller fails open (alert sent without dedup).
const matchTimeout = 100 * time.Millisecond

// mustCompile compiles a pattern with matchTimeout applied. All patterns —
// built-in and custom — must go through this so no match can run unbounded.
func mustCompile(pattern string) *regexp2.Regexp {
	re := regexp2.MustCompile(pattern, 0)
	re.MatchTimeout = matchTimeout
	return re
}

// NormalizerRule is a user-defined regex replacement applied before built-in logic.
type NormalizerRule struct {
	pattern     *regexp2.Regexp
	replacement string
}

func NewNormalizerRule(pattern, replacement string) NormalizerRule {
	return NormalizerRule{
		pattern:     mustCompile(pattern),
		replacement: replacement,
	}
}

var (
	// keyPattern matches valid key identifiers in key=value tokens.
	keyPattern = mustCompile(`\A[A-Za-z_][\w.\-]*\z`)

	// prePatterns and preReplacements are parallel; order is load-bearing.
	prePatterns = []*regexp2.Regexp{
		// 1. Combined date+time (ISO and slash-separated)
		mustCompile(`\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?`),
		// 2. Apache CLF timestamp (HAProxy appends fractional seconds)
		mustCompile(`\d{1,2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/\d{4}:\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?(?: +[+-]\d{4})?`),
		// 3. Syslog timestamp
		mustCompile(`(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +\d{1,2} +\d{2}:\d{2}:\d{2}`),
		// 4. URL — stops at quotes so a quoted URL ("http://…") never eats its
		// closing quote and breaks quote pairing for the rest of the line
		mustCompile(`(?:https?|wss?|ftp)://[^\s"'<>]+`),
		// 5. Email
		mustCompile(`\b[\w.+-]+@[\w.\-]+\.[A-Za-z]{2,}\b`),
		// 6. Java stack frame parens: (Foo.java:42)
		mustCompile(`\(\w+\.(?:java|kt|scala|py|js|cpp|c|go|rb|ts):\d+\)`),
		// 7. JVM synthetic lambda + memory address
		mustCompile(`\$\$Lambda\$\d+/0x[0-9a-fA-F]+`),
		// 8. UUID
		mustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`),
		// 8b. JWT / bearer token — "eyJ" is base64 for `{"`, so every JWT
		// starts with it. Must run before the hex/number rules shred the
		// dot-separated segments into fragments.
		mustCompile(`\beyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{4,}(?:\.[A-Za-z0-9_\-]*)?`),
		// 9. MAC address (before 0xHEX so colon-joined pairs are recognised first)
		mustCompile(`\b(?:[0-9a-fA-F]{2}[:\-]){5}[0-9a-fA-F]{2}\b`),
		// 9b. IPv6 — full 8-group form or compressed with "::". Must run here,
		// before the hex/IPv4/number rules split the address into fragments
		// (e.g. fe80::1ff:fe23:4567:890a → fe80::1ff:fe23:<N>:890a) that the
		// later whole-token IPv6 check can no longer recognise. Requires all 8
		// groups or a "::" so times (10:23:45) and MACs never match.
		mustCompile(`(?<![\w:.])(?:(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,6}:(?:[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,5})?|::[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,6})(?![\w:])`),
		// 10. 0x-prefixed hex
		mustCompile(`\b0x[0-9a-fA-F]+\b`),
		// 11. IPv4:port (before bare IPv4)
		mustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+\b`),
		// 12. IPv4
		mustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`),
		// 13. Windows path: C:\foo\bar
		mustCompile(`\b[A-Za-z]:\\[\w\\.\\-]+`),
		// 14. Unix path (negative lookbehind prevents matching after a word char or <)
		mustCompile(`(?<![\w<])/(?:[\w.\-]+/)+[\w.\-]*`),
		// 14b. Single-segment Unix path: /data, /health. Rule 14 needs two
		// segments; the same lookbehind still blocks and/or, I/O, HTTP/1.1.
		mustCompile(`(?<![\w<])/[\w.\-]{2,}`),
		// 14c. Semantic version: v2.14.3, 1.8.22-rc1. After the IP and path
		// rules so 1.2.3.4 stays <IP> and /app-1.2.3/lib stays one <PATH>.
		mustCompile(`\bv?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z][0-9A-Za-z.\-]*)?\b`),
		// 15. Date only: YYYY-MM-DD or YYYY/M/D (day/month may be unpadded)
		mustCompile(`\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b`),
		// 16. Time only: HH:MM:SS(.ms)
		mustCompile(`\b\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?\b`),
		// 16b. Time without seconds: HH:MM (16 already took HH:MM:SS; the
		// lookahead keeps H:M:S leftovers and ratios like 3:1 untouched)
		mustCompile(`\b\d{1,2}:\d{2}\b(?!:\d)`),
		// 17. Bare hex 7+ chars (short git SHA and up) — requires at least
		// one digit AND one letter so plain words and plain numbers survive
		mustCompile(`\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{7,}\b`),
		// 18. Size with unit, any case (kernel logs write kB)
		mustCompile(`\b\d+(?:\.\d+)?(?i:[kmgt]i?b|b)\b`),
		// 19. Duration with unit, including compound forms like 1h30m
		mustCompile(`\b(?:\d+(?:\.\d+)?(?:ns|us|ms|s|m|h|d))+\b`),
		// 20. Percent
		mustCompile(`\b\d+(?:\.\d+)?%`),
		// 21. Bare number — negative lookbehind prevents matching inside identifiers
		mustCompile(`(?<![\w<])-?\d+(?:\.\d+)?\b`),
	}

	// Literal replacement strings. Use replaceFunc so $ chars are never
	// misinterpreted as regexp2 group references.
	preReplacements = []string{
		"<TS>",               // 1
		"<TS>",               // 2
		"<TS>",               // 3
		"<URL>",              // 4
		"<EMAIL>",            // 5
		"(<FILE>:<LINE>)",    // 6
		"$$Lambda$<N>/<HEX>", // 7  (literal output — contains $ signs)
		"<UUID>",             // 8
		"<JWT>",              // 8b
		"<MAC>",              // 9
		"<IP6>",              // 9b
		"<HEX>",              // 10
		"<IP>:<PORT>",        // 11
		"<IP>",               // 12
		"<PATH>",             // 13
		"<PATH>",             // 14
		"<PATH>",             // 14b
		"<VER>",              // 14c
		"<TS>",               // 15
		"<TS>",               // 16
		"<TS>",               // 16b
		"<HEX>",              // 17
		"<SIZE>",             // 18
		"<DUR>",              // 19
		"<PCT>",              // 20
		"<N>",                // 21
	}

	// MEDIUM-mode patterns (also run at LOW).
	mediumAlphaHex8   = mustCompile(`\b[a-fA-F]{8,}\b`)
	mediumMixedHex47  = mustCompile(`\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{4,7}\b`)
	mediumIdentDigits = mustCompile(`(?<=[A-Za-z_])\d+`)
)

// replaceFunc replaces all matches with the literal string repl, bypassing
// regexp2's $ substitution rules so replacement strings are never mangled.
// The only possible error is a match timeout (see matchTimeout).
func replaceFunc(re *regexp2.Regexp, s, repl string) (string, error) {
	return re.ReplaceFunc(s, func(regexp2.Match) string { return repl }, -1, -1)
}

// LogMessageNormalizer reduces a raw log line to a stable structural pattern.
type LogMessageNormalizer struct {
	customRules []NormalizerRule
	mode        RestrictMode
	keepWords   map[string]struct{}
}

func NewLogMessageNormalizer(customRules []NormalizerRule, mode RestrictMode, alertKeywords []string) *LogMessageNormalizer {
	keep := make(map[string]struct{}, len(alertKeywords))
	for _, w := range alertKeywords {
		if t := strings.TrimSpace(strings.ToLower(w)); t != "" {
			keep[t] = struct{}{}
		}
	}
	return &LogMessageNormalizer{
		customRules: customRules,
		mode:        mode,
		keepWords:   keep,
	}
}

// NormalizeMessage returns an error only when a regex match timed out on this
// message (catastrophic backtracking — see matchTimeout). Callers must fail
// open on error: skip dedup and send the alert rather than suppress it.
func (n *LogMessageNormalizer) NormalizeMessage(message string) (string, error) {
	if message == "" {
		return "", nil
	}
	s := message
	var err error

	for _, r := range n.customRules {
		if s, err = replaceFunc(r.pattern, s, r.replacement); err != nil {
			return "", fmt.Errorf("custom rule %q: %w", r.pattern.String(), err)
		}
	}
	for i, re := range prePatterns {
		if s, err = replaceFunc(re, s, preReplacements[i]); err != nil {
			return "", fmt.Errorf("built-in rule %q: %w", re.String(), err)
		}
	}
	if n.mode == Medium || n.mode == Low {
		if s, err = n.mediumPass(s); err != nil {
			return "", err
		}
	}
	s = extractStructures(s)
	s = n.classifyTokens(s)
	if n.mode == Low {
		s = n.lowPass(s)
	}
	return strings.TrimSpace(s), nil
}

// ===================== MEDIUM pass =====================

func (n *LogMessageNormalizer) mediumPass(s string) (string, error) {
	var err error
	if s, err = replaceFunc(mediumAlphaHex8, s, "<HEX>"); err != nil {
		return "", fmt.Errorf("medium hex rule: %w", err)
	}
	if s, err = replaceFunc(mediumMixedHex47, s, "<HEX>"); err != nil {
		return "", fmt.Errorf("medium hex rule: %w", err)
	}
	if s, err = replaceFunc(mediumIdentDigits, s, "<N>"); err != nil {
		return "", fmt.Errorf("medium ident rule: %w", err)
	}
	return s, nil
}

// ===================== LOW pass =====================

func (n *LogMessageNormalizer) lowPass(s string) string {
	runes := []rune(s)
	var out []rune
	i := 0
	first := true

	for i < len(runes) {
		for i < len(runes) && isWhitespace(runes[i]) {
			i++
		}
		if i >= len(runes) {
			break
		}
		start := i
		for i < len(runes) && !isWhitespace(runes[i]) {
			i++
		}
		token := string(runes[start:i])
		if !first {
			out = append(out, ' ')
		}
		out = append(out, []rune(n.collapseLowToken(token))...)
		first = false
	}
	return string(out)
}

func (n *LogMessageNormalizer) collapseLowToken(token string) string {
	runes := []rune(token)
	start, end := peel(runes)
	prefix := string(runes[:start])
	core := string(runes[start:end])
	suffix := string(runes[end:])

	if len([]rune(core)) < 3 {
		return token
	}
	if isPlaceholder(core) || containsPlaceholder(core) {
		return token
	}
	if strings.ContainsRune(core, '=') {
		return token
	}
	for _, c := range core {
		if !isLetter(c) {
			return token
		}
	}
	if _, ok := n.keepWords[strings.ToLower(core)]; ok {
		return token
	}
	return prefix + "<TOK>" + suffix
}

// ===================== PASS 1b — balance-aware extraction =====================

func extractStructures(s string) string {
	runes := []rune(s)
	var out []rune
	i := 0
	n := len(runes)

	for i < n {
		c := runes[i]
		switch c {
		case '"', '\'':
			end := findStringEnd(runes, i, c)
			out = append(out, []rune("<STR>")...)
			i = end + 1
		case '{':
			end := findBalanced(runes, i, '{', '}')
			if end > i {
				out = append(out, []rune("<JSON>")...)
				i = end + 1
			} else {
				out = append(out, c)
				i++
			}
		case '[':
			end := findBalanced(runes, i, '[', ']')
			if end > i {
				inner := string(runes[i+1 : end])
				out = append(out, []rune(classifyBracketed(inner))...)
				i = end + 1
			} else {
				out = append(out, c)
				i++
			}
		default:
			out = append(out, c)
			i++
		}
	}
	return string(out)
}

func findStringEnd(runes []rune, start int, quote rune) int {
	n := len(runes)
	i := start + 1
	for i < n {
		c := runes[i]
		if c == '\\' && i+1 < n {
			i += 2
			continue
		}
		if c == quote {
			return i
		}
		i++
	}
	return n - 1
}

func findBalanced(runes []rune, start int, open, close rune) int {
	depth := 0
	n := len(runes)
	for i := start; i < n; i++ {
		c := runes[i]
		if c == '"' || c == '\'' {
			i = findStringEnd(runes, i, c)
			continue
		}
		if c == open {
			depth++
		} else if c == close {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func classifyBracketed(inner string) string {
	runes := []rune(inner)
	for i := 0; i+1 < len(runes); i++ {
		if runes[i] == ':' && isDigit(runes[i+1]) {
			return "<TS>"
		}
	}
	if strings.ContainsRune(inner, ',') {
		return "<ARR>"
	}
	return "[" + inner + "]"
}

// ===================== PASS 2 — token classification =====================

func (n *LogMessageNormalizer) classifyTokens(s string) string {
	runes := []rune(s)
	var out []rune
	i := 0
	first := true

	for i < len(runes) {
		for i < len(runes) && isWhitespace(runes[i]) {
			i++
		}
		if i >= len(runes) {
			break
		}
		start := i
		for i < len(runes) && !isWhitespace(runes[i]) {
			i++
		}
		token := string(runes[start:i])
		if !first {
			out = append(out, ' ')
		}
		out = append(out, []rune(n.classifyToken(token))...)
		first = false
	}
	return string(out)
}

func (n *LogMessageNormalizer) classifyToken(token string) string {
	runes := []rune(token)
	start, end := peel(runes)
	prefix := string(runes[:start])
	core := string(runes[start:end])
	suffix := string(runes[end:])
	return prefix + n.classifyCore(core) + suffix
}

func (n *LogMessageNormalizer) classifyCore(core string) string {
	if core == "" {
		return core
	}
	if isPlaceholder(core) {
		return core
	}
	if isIPv6(core) {
		return "<IP6>"
	}
	eq := strings.IndexRune(core, '=')
	if eq > 0 && eq < len([]rune(core))-1 {
		key := core[:eq]
		value := core[eq+1:]
		m, _ := keyPattern.MatchString(key)
		if m {
			return strings.ToLower(key) + "=" + classifyValue(value)
		}
	}
	return lowercaseKeepingPlaceholders(core)
}

func classifyValue(value string) string {
	if value == "" {
		return "<VAL>"
	}
	if isPlaceholder(value) {
		return value
	}
	if containsPlaceholder(value) {
		return lowercaseKeepingPlaceholders(value)
	}
	return "<VAL>"
}

// ===================== placeholder helpers =====================

func isPlaceholder(s string) bool {
	runes := []rune(s)
	n := len(runes)
	if n < 3 || runes[0] != '<' || runes[n-1] != '>' {
		return false
	}
	for _, c := range runes[1 : n-1] {
		if !isPlaceholderChar(c) {
			return false
		}
	}
	return true
}

func containsPlaceholder(s string) bool {
	runes := []rune(s)
	for i := 0; i+2 < len(runes); i++ {
		if runes[i] == '<' && scanPlaceholder(runes, i) > i {
			return true
		}
	}
	return false
}

func scanPlaceholder(runes []rune, from int) int {
	n := len(runes)
	if from+2 >= n || runes[from] != '<' {
		return -1
	}
	for i := from + 1; i < n; i++ {
		c := runes[i]
		if c == '>' {
			if i > from+1 {
				return i
			}
			return -1
		}
		if !isPlaceholderChar(c) {
			return -1
		}
	}
	return -1
}

func isPlaceholderChar(c rune) bool {
	return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':' || c == '<' || c == '>'
}

func lowercaseKeepingPlaceholders(s string) string {
	runes := []rune(s)
	var out []rune
	i := 0
	for i < len(runes) {
		if runes[i] == '<' {
			end := scanPlaceholder(runes, i)
			if end > i {
				out = append(out, runes[i:end+1]...)
				i = end + 1
				continue
			}
		}
		out = append(out, toLower(runes[i]))
		i++
	}
	return string(out)
}

// ===================== IPv6 predicate =====================

func isIPv6(s string) bool {
	runes := []rune(s)
	n := len(runes)
	if n < 3 {
		return false
	}
	colons := 0
	for _, c := range runes {
		if c == ':' {
			colons++
		} else if !isHexChar(c) {
			return false
		}
	}
	if strings.Contains(s, "::") {
		return colons >= 2 && colons <= 7
	}
	return colons == 7
}

// ===================== char helpers =====================

func peel(runes []rune) (start, end int) {
	end = len(runes)
	for start < end && isPeelable(runes[start]) {
		start++
	}
	for end > start && isPeelable(runes[end-1]) {
		end--
	}
	return start, end
}

func isPeelable(c rune) bool {
	switch c {
	case ',', ';', '.', '!', '?', '(', ')', '[', ']', '{', '}':
		return true
	}
	return false
}

func isWhitespace(c rune) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
func isDigit(c rune) bool      { return c >= '0' && c <= '9' }
func isLetter(c rune) bool     { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isHexChar(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
func toLower(c rune) rune {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}
