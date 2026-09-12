package update

import "testing"

func TestUpgradeHint(t *testing.T) {
	tests := []struct {
		name    string
		goos    string
		exePath string
		want    string
	}{
		{
			name:    "darwin Cellar",
			goos:    "darwin",
			exePath: "/usr/local/Cellar/mcpwarp/0.1.0/bin/mcpwarp",
			want:    "brew upgrade --cask mcpwarp",
		},
		{
			name:    "darwin Caskroom",
			goos:    "darwin",
			exePath: "/opt/homebrew/Caskroom/mcpwarp/0.1.0/mcpwarp",
			want:    "brew upgrade --cask mcpwarp",
		},
		{
			name:    "darwin /usr/local/bin (not brew)",
			goos:    "darwin",
			exePath: "/usr/local/bin/mcpwarp",
			want:    "download from " + releaseURL,
		},
		{
			name:    "windows scoop",
			goos:    "windows",
			exePath: `C:\Users\ana\scoop\apps\mcpwarp\current\mcpwarp.exe`,
			want:    "scoop update mcpwarp",
		},
		{
			name:    "windows plain path",
			goos:    "windows",
			exePath: `C:\Program Files\mcpwarp\mcpwarp.exe`,
			want:    "download from " + releaseURL,
		},
		{
			name:    "windows scoop, forward slashes",
			goos:    "windows",
			exePath: "C:/Users/ana/scoop/apps/mcpwarp/current/mcpwarp.exe",
			want:    "scoop update mcpwarp",
		},
		{
			name:    "windows scoop, mixed case",
			goos:    "windows",
			exePath: `C:\Users\ana\Scoop\apps\mcpwarp\current\mcpwarp.exe`,
			want:    "scoop update mcpwarp",
		},
		{
			name:    "linux /usr/bin",
			goos:    "linux",
			exePath: "/usr/bin/mcpwarp",
			want:    "use your package manager (apt/dnf/apk) or download from " + releaseURL,
		},
		{
			name:    "linux ~/bin",
			goos:    "linux",
			exePath: "/home/ana/bin/mcpwarp",
			want:    "download from " + releaseURL,
		},
		{
			name:    "linux /usr/local/bin",
			goos:    "linux",
			exePath: "/usr/local/bin/mcpwarp",
			want:    "download from " + releaseURL,
		},
		{
			name:    "linux path merely prefixed by /usr/bin (not the directory itself)",
			goos:    "linux",
			exePath: "/usr/bin-local/mcpwarp",
			want:    "download from " + releaseURL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UpgradeHint(tt.goos, tt.exePath); got != tt.want {
				t.Errorf("UpgradeHint(%q, %q) = %q, want %q", tt.goos, tt.exePath, got, tt.want)
			}
		})
	}
}
