# mcpwarp

mcpwarp exposes local MCP servers (stdio or HTTP) at public URLs through the mcpwarp tunnel. A server's public URL is tied to its name — renaming it in config assigns a new URL.

[mcpwarp.io](https://mcpwarp.io) · [docs](https://mcpwarp.io/docs/get-started)

## Install

### macOS and Linux (Homebrew)

Homebrew 6+ requires trusting non-official taps once before installing from them:

```sh
brew tap mcpwarp/tap
brew trust --tap mcpwarp/tap
brew install --cask mcpwarp
```

Upgrade with `brew upgrade --cask mcpwarp`.

### Windows (Scoop)

Scoop is the only supported Windows install path besides the manual zip download.

```powershell
scoop bucket add mcpwarp https://github.com/mcpwarp/scoop-bucket
scoop install mcpwarp
```

Upgrade with `scoop update mcpwarp`.

### Debian/Ubuntu, Fedora/RHEL, Alpine

Download the `.deb`, `.rpm`, or `.apk` for your architecture from the [latest release](https://github.com/mcpwarp/cli/releases/latest), then:

```sh
sudo apt install ./mcpwarp_<version>_amd64.deb              # Debian/Ubuntu
sudo dnf install ./mcpwarp-<version>-1.x86_64.rpm           # Fedora/RHEL
sudo apk add --allow-untrusted mcpwarp_<version>_x86_64.apk # Alpine
```

### Manual download

Grab the tar.gz (macOS/Linux) or zip (Windows) for your OS/arch from the [latest release](https://github.com/mcpwarp/cli/releases/latest), download `checksums.txt` into the same directory, then verify the archive from that directory before putting the binary on your `PATH`:

```sh
shasum -a 256 -c checksums.txt --ignore-missing   # macOS
sha256sum -c checksums.txt --ignore-missing       # Linux
```

```powershell
Get-FileHash .\mcpwarp_<version>_windows_amd64.zip -Algorithm SHA256   # Windows, compare against the matching line in checksums.txt
```

On macOS, the binary is unsigned/un-notarized, so a manual download needs the quarantine attribute removed once:

```sh
xattr -d com.apple.quarantine ./mcpwarp
```

### Staying up to date

mcpwarp checks for updates once a day when run on an interactive terminal, and prints the notice with the upgrade command for your install method. Opt out with `MCPWARP_NO_UPDATE_NOTIFIER=1`; the check is also skipped when `CI` is set.

## Quick start

```sh
mcpwarp login
```

Create `~/.mcpwarp/config.json` (or pass `--config <path>` to use a different location):

```json
{
  "servers": [
    {
      "name": "blender",
      "kind": "stdio",
      "command": "uvx",
      "args": ["blender-mcp"]
    },
    {
      "name": "notes",
      "kind": "http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  ]
}
```

A `stdio` server is defined by `name`, `kind`, `command`, and optionally `args`, `env`, `cwd`. An `http` server is defined by `name`, `kind`, and `url`.

```sh
mcpwarp up          # registers each server, prints its URL; TUI unless --no-tui
mcpwarp dashboard   # opens the mcpwarp dashboard in your browser
mcpwarp status      # config path, server table, login status
```

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `MCPWARP_AUTH_URL` | auth server | `https://auth.mcpwarp.io` |
| `MCPWARP_CONNECT_URL` | tunnel WebSocket URL | `wss://connect.mcpwarp.io` |
| `MCPWARP_WEB_URL` | dashboard URL | `https://mcpwarp.io` |
| `MCPWARP_TOKEN` | personal access token, skips login | — |
| `MCPWARP_NO_UPDATE_NOTIFIER` | disable the update check | — |

## Development

```sh
make help    # list all targets
make build   # build bin/mcpwarp
make test    # run tests with -race
```

See [docs/DESIGN.md](docs/DESIGN.md) for the architecture.

## License

MIT
