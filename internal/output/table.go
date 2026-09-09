package output

import (
	"strings"
	"unicode/utf8"
)

// TableRow is one row of the three-column name/kind/target table shared by
// `mcpwarp up` and `mcpwarp status` (Node's src/cli/table.ts).
type TableRow struct {
	Name string
	Kind string
	URL  string
}

// TableHeaders is the three column headers.
type TableHeaders [3]string

var defaultHeaders = TableHeaders{"NAME", "KIND", "URL"}

// FormatTable pads each column to its widest cell (header included) and
// joins rows with newlines — no box drawing, so it stays readable piped or
// logged. Pass headers as the zero value to get the NAME/KIND/URL default.
func FormatTable(rows []TableRow, headers TableHeaders) string {
	if headers == (TableHeaders{}) {
		headers = defaultHeaders
	}

	nameWidth := utf8.RuneCountInString(headers[0])
	kindWidth := utf8.RuneCountInString(headers[1])
	for _, r := range rows {
		if n := utf8.RuneCountInString(r.Name); n > nameWidth {
			nameWidth = n
		}
		if n := utf8.RuneCountInString(r.Kind); n > kindWidth {
			kindWidth = n
		}
	}

	formatRow := func(name, kind, url string) string {
		return padEnd(name, nameWidth) + "  " + padEnd(kind, kindWidth) + "  " + url
	}

	lines := []string{formatRow(headers[0], headers[1], headers[2])}
	for _, r := range rows {
		lines = append(lines, formatRow(r.Name, r.Kind, r.URL))
	}
	return strings.Join(lines, "\n")
}

func padEnd(s string, width int) string {
	n := utf8.RuneCountInString(s)
	if n >= width {
		return s
	}
	return s + strings.Repeat(" ", width-n)
}
