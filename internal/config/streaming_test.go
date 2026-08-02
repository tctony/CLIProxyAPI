package config

import "testing"

func TestParseConfigBytesStreamingIdleTimeoutSeconds(t *testing.T) {
	for name, input := range map[string]string{
		"legacy": "streaming:\n  stream-idle-timeout-seconds: 45\n",
		"v8":     "requests:\n  streaming:\n    stream-idle-timeout-seconds: 45\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := ParseConfigBytes([]byte(input))
			if err != nil {
				t.Fatalf("ParseConfigBytes() error = %v", err)
			}
			if got := cfg.Streaming.StreamIdleTimeoutSeconds; got != 45 {
				t.Fatalf("stream idle timeout seconds = %d, want 45", got)
			}
		})
	}
}
