package logging

import (
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func TestLogFormatterPrintsVersionField(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Time = time.Date(2026, 6, 9, 11, 10, 2, 0, time.Local)
	entry.Level = log.InfoLevel
	entry.Message = "fetched latest antigravity version"
	entry.Data["version"] = "2.1.0"

	formatted, errFormat := (&LogFormatter{}).Format(entry)
	if errFormat != nil {
		t.Fatalf("Format() error = %v", errFormat)
	}

	line := string(formatted)
	if !strings.Contains(line, "version=2.1.0") {
		t.Fatalf("formatted line %q missing version field", line)
	}
}

func TestLogFormatterPrintsMediaForwardingFields(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Time = time.Date(2026, 7, 25, 7, 36, 4, 0, time.Local)
	entry.Level = log.InfoLevel
	entry.Message = "codex live remote media forwarding started"
	entry.Data["credential"] = "Voice credential\nsecondary"
	entry.Data["connection"] = "via socks5 proxy"
	entry.Data["proxy_scheme"] = "socks5"
	entry.Data["remote_transport"] = "tcp"
	entry.Data["media_session_id"] = "media-session-id"
	entry.Data["call_id"] = "call-id"
	entry.Data["peer"] = "remote"
	entry.Data["state"] = "connected"

	formatted, errFormat := (&LogFormatter{}).Format(entry)
	if errFormat != nil {
		t.Fatalf("Format() error = %v", errFormat)
	}

	line := string(formatted)
	for _, want := range []string{
		`credential="Voice credential\nsecondary"`,
		`connection="via socks5 proxy"`,
		`proxy_scheme="socks5"`,
		`remote_transport="tcp"`,
		`media_session_id="media-session-id"`,
		`call_id="call-id"`,
		`peer="remote"`,
		`state="connected"`,
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("formatted line %q missing %s", line, want)
		}
	}
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("formatted line contains an unescaped newline: %q", line)
	}
}

func TestLogFormatterPrintsSafeCodexWebsocketDiagnosticFields(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Time = time.Date(2026, 7, 30, 6, 21, 18, 0, time.Local)
	entry.Level = log.WarnLevel
	entry.Message = "codex websocket upstream read stopped"
	entry.Data["session"] = "session-id\nsecond-line"
	entry.Data["active_response"] = true
	entry.Data["event"] = "response.created"
	entry.Data["item_type"] = "reasoning"
	entry.Data["output_index"] = int64(0)
	entry.Data["last_event"] = "response.output_text.delta"
	entry.Data["sequence"] = int64(11)
	entry.Data["frame_bytes"] = 512
	entry.Data["previous_gap"] = 250 * time.Millisecond
	entry.Data["frame_count"] = uint64(37)
	entry.Data["cumulative_size"] = uint64(8192)
	entry.Data["last_frame_ago"] = 73 * time.Second
	entry.Data["byte_count"] = uint64(8192)
	entry.Data["error_kind"] = "abnormal_close"
	entry.Data["close_code"] = 1006
	entry.Data["turn"] = uint64(104)
	entry.Data["turn_started_at"] = "2026-07-30T05:56:26.596+08:00"
	entry.Data["turn_elapsed"] = 156408 * time.Millisecond
	entry.Data["turn_last_event"] = "response.output_text.delta"
	entry.Data["turn_last_frame_ago"] = 113 * time.Second
	entry.Data["turn_frame_count"] = uint64(41)
	entry.Data["turn_byte_count"] = uint64(4096)
	entry.Data["turn_previous_gap"] = 10 * time.Second
	entry.Data["first_frame"] = false
	entry.Data["connection_elapsed"] = 34*time.Minute + 29*time.Second
	entry.Data["observed_at"] = "2026-07-30T05:59:03.004+08:00"
	entry.Data["control"] = "ping"
	entry.Data["control_bytes"] = 8
	entry.Data["pong_sent"] = true
	entry.Data["payload"] = "secret output"

	formatted, errFormat := (&LogFormatter{}).Format(entry)
	if errFormat != nil {
		t.Fatalf("Format() error = %v", errFormat)
	}

	line := string(formatted)
	for _, want := range []string{
		`session="session-id\nsecond-line"`,
		"active_response=true",
		`event="response.created"`,
		`item_type="reasoning"`,
		"output_index=0",
		`last_event="response.output_text.delta"`,
		"sequence=11",
		"frame_bytes=512",
		"previous_gap=250ms",
		"frame_count=37",
		"cumulative_size=8192",
		"last_frame_ago=1m13s",
		"byte_count=8192",
		`error_kind="abnormal_close"`,
		"close_code=1006",
		"turn=104",
		`turn_started_at="2026-07-30T05:56:26.596+08:00"`,
		"turn_elapsed=2m36.408s",
		`turn_last_event="response.output_text.delta"`,
		"turn_last_frame_ago=1m53s",
		"turn_frame_count=41",
		"turn_byte_count=4096",
		"turn_previous_gap=10s",
		"first_frame=false",
		"connection_elapsed=34m29s",
		`observed_at="2026-07-30T05:59:03.004+08:00"`,
		`control="ping"`,
		"control_bytes=8",
		"pong_sent=true",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("formatted line %q missing %s", line, want)
		}
	}
	if strings.Contains(line, "secret output") || strings.Contains(line, "payload=") {
		t.Fatalf("formatted line contains payload data: %q", line)
	}
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("formatted line contains an unescaped newline: %q", line)
	}
}

func TestLogFormatterPrintsPluginFields(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Time = time.Date(2026, 6, 25, 20, 10, 0, 0, time.Local)
	entry.Level = log.InfoLevel
	entry.Message = "pluginhost: plugin loaded"
	entry.Data["plugin_id"] = "sample-provider"
	entry.Data["plugin_name"] = "Sample Provider"
	entry.Data["version"] = "0.2.0"
	entry.Data["active_version"] = "0.1.0"
	entry.Data["retired_version"] = "0.2.0"
	entry.Data["path"] = "plugins/windows/amd64/sample-provider-v0.2.0.dll"
	entry.Data["active_path"] = "plugins/windows/amd64/sample-provider-v0.1.0.dll"
	entry.Data["retired_path"] = "plugins/windows/amd64/sample-provider-v0.2.0.dll"

	formatted, errFormat := (&LogFormatter{}).Format(entry)
	if errFormat != nil {
		t.Fatalf("Format() error = %v", errFormat)
	}

	line := string(formatted)
	for _, want := range []string{
		"plugin_id=sample-provider",
		"plugin_name=Sample Provider",
		"version=0.2.0",
		"active_version=0.1.0",
		"retired_version=0.2.0",
		"path=plugins/windows/amd64/sample-provider-v0.2.0.dll",
		"active_path=plugins/windows/amd64/sample-provider-v0.1.0.dll",
		"retired_path=plugins/windows/amd64/sample-provider-v0.2.0.dll",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("formatted line %q missing %s", line, want)
		}
	}
}

func TestLogFormatterOmitsGenericPathField(t *testing.T) {
	entry := log.NewEntry(log.New())
	entry.Time = time.Date(2026, 6, 25, 20, 20, 0, 0, time.Local)
	entry.Level = log.WarnLevel
	entry.Message = "failed to roll back token"
	entry.Data["path"] = "auths/private-token.json"
	entry.Data["active_path"] = "plugins/windows/amd64/sample-provider-v0.1.0.dll"
	entry.Data["retired_path"] = "plugins/windows/amd64/sample-provider-v0.2.0.dll"

	formatted, errFormat := (&LogFormatter{}).Format(entry)
	if errFormat != nil {
		t.Fatalf("Format() error = %v", errFormat)
	}

	line := string(formatted)
	for _, forbidden := range []string{"path=", "active_path=", "retired_path="} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("formatted line %q contains generic %s field", line, forbidden)
		}
	}
}
