package bonjour

import (
	"strconv"
	"strings"
)

// trimDot is used to trim the dots from the start or end of a string
func trimDot(s string) string {
	return strings.Trim(s, ".")
}

// canonicalName reverses the \X and \DDD escaping the dns package applies when
// unpacking names and lower-cases the letters A to Z, so names compare as they do
// on the wire (RFC 6762 16). A dot or backslash inside a label stays escaped, which
// keeps it apart from the dots between labels.
func canonicalName(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\\' || i+1 == len(s) {
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			b = append(b, c)
			continue
		}
		i++
		c = s[i]
		if i+3 <= len(s) {
			if n, err := strconv.ParseUint(s[i:i+3], 10, 8); err == nil {
				c = byte(n)
				i += 2
			}
		}
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c == '.' || c == '\\' {
			b = append(b, '\\')
		}
		b = append(b, c)
	}
	return string(b)
}
