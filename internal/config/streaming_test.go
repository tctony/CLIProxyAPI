package config

import "testing"

func TestParseConfigBytesStreamingIdleTimeoutSeconds(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte("streaming:\n  stream-idle-timeout-seconds: 45\n"))
	if err != nil {
		t.Fatalf("ParseConfigBytes() error = %v", err)
	}
	if got := cfg.Streaming.StreamIdleTimeoutSeconds; got != 45 {
		t.Fatalf("stream idle timeout seconds = %d, want 45", got)
	}
}
