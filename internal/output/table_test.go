package output

import (
	"strings"
	"testing"
)

func TestFormatTable(t *testing.T) {
	rows := []TableRow{
		{Name: "blender", Kind: "stdio", URL: "https://7f3a.mcpwarp.io/mcp"},
		{Name: "notes", Kind: "http", URL: "https://91bc.mcpwarp.io/mcp"},
	}

	t.Run("pads columns to the widest cell and includes a header row", func(t *testing.T) {
		text := FormatTable(rows, TableHeaders{})
		lines := strings.Split(text, "\n")
		if len(lines) != 3 {
			t.Fatalf("expected 3 lines, got %d: %q", len(lines), text)
		}
		if !strings.HasPrefix(lines[0], "NAME") {
			t.Errorf("header line: %q", lines[0])
		}
	})

	t.Run("renders just the header for an empty row set", func(t *testing.T) {
		text := FormatTable(nil, TableHeaders{})
		if strings.Contains(text, "\n") {
			t.Errorf("expected single line, got %q", text)
		}
	})

	t.Run("matches the exact padded layout", func(t *testing.T) {
		want := strings.Join([]string{
			"NAME     KIND   URL",
			"blender  stdio  https://7f3a.mcpwarp.io/mcp",
			"notes    http   https://91bc.mcpwarp.io/mcp",
		}, "\n")
		if got := FormatTable(rows, TableHeaders{}); got != want {
			t.Errorf("got:\n%s\nwant:\n%s", got, want)
		}
	})

	t.Run("matches the exact header-only layout for an empty row set", func(t *testing.T) {
		if got := FormatTable(nil, TableHeaders{}); got != "NAME  KIND  URL" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("accepts a custom header triple, e.g. status's NAME/KIND/TARGET", func(t *testing.T) {
		text := FormatTable([]TableRow{{Name: "blender", Kind: "stdio", URL: "uvx blender-mcp"}}, TableHeaders{"NAME", "KIND", "TARGET"})
		lines := strings.Split(text, "\n")
		if !strings.HasPrefix(lines[0], "NAME") || !strings.Contains(lines[0], "TARGET") {
			t.Errorf("header line: %q", lines[0])
		}
		if !strings.Contains(lines[1], "uvx blender-mcp") {
			t.Errorf("row line: %q", lines[1])
		}
	})

	t.Run("aligns a non-ASCII name by rune count, not byte count", func(t *testing.T) {
		// "café" is 4 runes but 5 bytes (é is 2 UTF-8 bytes); a byte-counted
		// pad would under-pad this column by one space relative to a
		// same-rune-width ASCII name.
		text := FormatTable([]TableRow{
			{Name: "café", Kind: "stdio", URL: "uvx x"},
			{Name: "note", Kind: "http", URL: "https://x"},
		}, TableHeaders{})
		lines := strings.Split(text, "\n")
		if len(lines) != 3 {
			t.Fatalf("expected 3 lines, got %d: %q", len(lines), text)
		}
		wantGap := "  " // NAME column width 4, one space short of that plus the 2-space separator
		if !strings.HasPrefix(lines[1], "café"+wantGap+"stdio") {
			t.Errorf("got %q", lines[1])
		}
		if !strings.HasPrefix(lines[2], "note"+wantGap+"http ") {
			t.Errorf("got %q", lines[2])
		}
	})
}
