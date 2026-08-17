package server

import "testing"

func TestSlugify(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"Demo Project", "demo-project"},
		{"  Leading And Trailing  ", "leading-and-trailing"},
		{"Weird!!Chars??Here", "weird-chars-here"},
		{"already-a-slug", "already-a-slug"},
	}
	for _, tt := range tests {
		if got := slugify(tt.name); got != tt.want {
			t.Errorf("slugify(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestMapWorkerStatus(t *testing.T) {
	tests := []struct {
		worker string
		want   string
	}{
		{"running", "running"},
		{"exited", "failed"},
		{"pending", "scheduling"},
		{"unknown", "scheduling"},
	}
	for _, tt := range tests {
		if got := string(mapWorkerStatus(tt.worker)); got != tt.want {
			t.Errorf("mapWorkerStatus(%q) = %q, want %q", tt.worker, got, tt.want)
		}
	}
}

func TestRevisionCursorRoundTrip(t *testing.T) {
	cursor := encodeRevisionCursor(42)
	got, err := decodeRevisionCursor(cursor)
	if err != nil {
		t.Fatalf("decodeRevisionCursor: %v", err)
	}
	if got == nil || *got != 42 {
		t.Fatalf("decodeRevisionCursor(%q) = %v, want 42", cursor, got)
	}
}

func TestDecodeRevisionCursor_Empty(t *testing.T) {
	got, err := decodeRevisionCursor("")
	if err != nil {
		t.Fatalf("decodeRevisionCursor(\"\"): %v", err)
	}
	if got != nil {
		t.Fatalf("decodeRevisionCursor(\"\") = %v, want nil", got)
	}
}

func TestDecodeRevisionCursor_Invalid(t *testing.T) {
	if _, err := decodeRevisionCursor("not-valid-base64!!"); err == nil {
		t.Fatal("expected an invalid cursor to fail to decode")
	}
}
