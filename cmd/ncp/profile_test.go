package main

import "testing"

func TestMaskSecret(t *testing.T) {
	cases := map[string]string{
		"":                   "",
		"short":              "****",
		"12345678":           "****",
		"123456789":          "1234****6789",
		"abcdefghijklmnop":   "abcd****mnop",
		"AKIAVERYLONGSECRET": "AKIA****CRET",
	}
	for in, want := range cases {
		if got := maskSecret(in); got != want {
			t.Errorf("maskSecret(%q)=%q, want %q", in, got, want)
		}
	}
}
