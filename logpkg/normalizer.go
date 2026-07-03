package logpkg

import (
	"strings"

	"github.com/dlclark/regexp2"
)

// NormalizerRule is a user-defined regex replacement applied before built-in logic.
type NormalizerRule struct {
	pattern     *regexp2.Regexp
	replacement string
}

func NewNormalizerRule(pattern, replacement string) NormalizerRule {
	return NormalizerRule{
		pattern:     regexp2.MustCompile(pattern, 0),
		replacement: replacement,
	}
}

var (
	// keyPattern matches valid key identifiers in key=value tokens.
	keyPattern = regexp2.MustCompile(`\A[A-Za-z_][\w.\-]*\z`, 0)

	// prePatterns and preReplacements are parallel; order is load-bearing.
	prePatterns = []*regexp2.Regexp{
		// 1. Combined date+time (ISO and slash-separated)
		regexp2.MustCompile(`\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?`, 0),
		// 2. Apache CLF timestamp (HAProxy appends fractional seconds)
		regexp2.MustCompile(`\d{1,2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/\d{4}:\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?(?: +[+-]\d{4})?`, 0),
		// 3. Syslog timestamp
		regexp2.MustCompile(`(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +\d{1,2} +\d{2}:\d{2}:\d{2}`, 0),
		// 4. URL — stops at quotes so a quoted URL ("http://…") never eats its
		// closing quote and breaks quote pairing for the rest of the line
		regexp2.MustCompile(`(?:https?|wss?|ftp)://[^\s"'<>]+`, 0),
		// 5. Email
		regexp2.MustCompile(`\b[\w.+-]+@[\w.\-]+\.[A-Za-z]{2,}\b`, 0),
		// 6. Java stack frame parens: (Foo.java:42)
		regexp2.MustCompile(`\(\w+\.(?:java|kt|scala|py|js|cpp|c|go|rb|ts):\d+\)`, 0),
		// 7. JVM synthetic lambda + memory address
		regexp2.MustCompile(`\$\$Lambda\$\d+/0x[0-9a-fA-F]+`, 0),
		// 8. UUID
		regexp2.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`, 0),
		// 8b. JWT / bearer token — "eyJ" is base64 for `{"`, so every JWT
		// starts with it. Must run before the hex/number rules shred the
		// dot-separated segments into fragments.
		regexp2.MustCompile(`\beyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{4,}(?:\.[A-Za-z0-9_\-]*)?`, 0),
		// 9. MAC address (before 0xHEX so colon-joined pairs are recognised first)
		regexp2.MustCompile(`\b(?:[0-9a-fA-F]{2}[:\-]){5}[0-9a-fA-F]{2}\b`, 0),
		// 9b. IPv6 — full 8-group form or compressed with "::". Must run here,
		// before the hex/IPv4/number rules split the address into fragments
		// (e.g. fe80::1ff:fe23:4567:890a → fe80::1ff:fe23:<N>:890a) that the
		// later whole-token IPv6 check can no longer recognise. Requires all 8
		// groups or a "::" so times (10:23:45) and MACs never match.
		regexp2.MustCompile(`(?<![\w:.])(?:(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,6}:(?:[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,5})?|::[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,6})(?![\w:])`, 0),
		// 10. 0x-prefixed hex
		regexp2.MustCompile(`\b0x[0-9a-fA-F]+\b`, 0),
		// 11. IPv4:port (before bare IPv4)
		regexp2.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+\b`, 0),
		// 12. IPv4
		regexp2.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`, 0),
		// 13. Windows path: C:\foo\bar
		regexp2.MustCompile(`\b[A-Za-z]:\\[\w\\.\\-]+`, 0),
		// 14. Unix path (negative lookbehind prevents matching after a word char or <)
		regexp2.MustCompile(`(?<![\w<])/(?:[\w.\-]+/)+[\w.\-]*`, 0),
		// 14b. Single-segment Unix path: /data, /health. Rule 14 needs two
		// segments; the same lookbehind still blocks and/or, I/O, HTTP/1.1.
		regexp2.MustCompile(`(?<![\w<])/[\w.\-]{2,}`, 0),
		// 14c. Semantic version: v2.14.3, 1.8.22-rc1. After the IP and path
		// rules so 1.2.3.4 stays <IP> and /app-1.2.3/lib stays one <PATH>.
		regexp2.MustCompile(`\bv?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z][0-9A-Za-z.\-]*)?\b`, 0),
		// 15. Date only: YYYY-MM-DD or YYYY/M/D (day/month may be unpadded)
		regexp2.MustCompile(`\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b`, 0),
		// 16. Time only: HH:MM:SS(.ms)
		regexp2.MustCompile(`\b\d{1,2}:\d{2}:\d{2}(?:[.,]\d+)?\b`, 0),
		// 16b. Time without seconds: HH:MM (16 already took HH:MM:SS; the
		// lookahead keeps H:M:S leftovers and ratios like 3:1 untouched)
		regexp2.MustCompile(`\b\d{1,2}:\d{2}\b(?!:\d)`, 0),
		// 17. Bare hex 7+ chars (short git SHA and up) — requires at least
		// one digit AND one letter so plain words and plain numbers survive
		regexp2.MustCompile(`\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{7,}\b`, 0),
		// 18. Size with unit, any case (kernel logs write kB)
		regexp2.MustCompile(`\b\d+(?:\.\d+)?(?i:[kmgt]i?b|b)\b`, 0),
		// 19. Duration with unit, including compound forms like 1h30m
		regexp2.MustCompile(`\b(?:\d+(?:\.\d+)?(?:ns|us|ms|s|m|h|d))+\b`, 0),
		// 20. Percent
		regexp2.MustCompile(`\b\d+(?:\.\d+)?%`, 0),
		// 21. Bare number — negative lookbehind prevents matching inside identifiers
		regexp2.MustCompile(`(?<![\w<])-?\d+(?:\.\d+)?\b`, 0),
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
	mediumAlphaHex8   = regexp2.MustCompile(`\b[a-fA-F]{8,}\b`, 0)
	mediumMixedHex47  = regexp2.MustCompile(`\b(?=[0-9a-fA-F]*\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{4,7}\b`, 0)
	mediumIdentDigits = regexp2.MustCompile(`(?<=[A-Za-z_])\d+`, 0)
)

// replaceFunc replaces all matches with the literal string repl, bypassing
// regexp2's $ substitution rules so replacement strings are never mangled.
func replaceFunc(re *regexp2.Regexp, s, repl string) string {
	result, _ := re.ReplaceFunc(s, func(regexp2.Match) string { return repl }, -1, -1)
	return result
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

func (n *LogMessageNormalizer) NormalizeMessage(message string) string {
	if message == "" {
		return ""
	}
	s := message

	for _, r := range n.customRules {
		s = replaceFunc(r.pattern, s, r.replacement)
	}
	for i, re := range prePatterns {
		s = replaceFunc(re, s, preReplacements[i])
	}
	if n.mode == Medium || n.mode == Low {
		s = n.mediumPass(s)
	}
	s = extractStructures(s)
	s = n.classifyTokens(s)
	if n.mode == Low {
		s = n.lowPass(s)
	}
	return strings.TrimSpace(s)
}

// ===================== MEDIUM pass =====================

func (n *LogMessageNormalizer) mediumPass(s string) string {
	s = replaceFunc(mediumAlphaHex8, s, "<HEX>")
	s = replaceFunc(mediumMixedHex47, s, "<HEX>")
	s = replaceFunc(mediumIdentDigits, s, "<N>")
	return s
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
