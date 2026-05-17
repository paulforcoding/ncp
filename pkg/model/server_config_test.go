package model

import (
	"encoding/json"
	"testing"
)

func TestServerConfigSerialization(t *testing.T) {
	sc := ServerConfig{
		ProgramLogLevel:   "debug",
		ProgramLogOutput:  "/var/log/ncp.log",
		FileLogEnabled:    true,
		FileLogOutput:     "console",
		FileLogInterval:   10,
		ProgressStorePath: "/tmp/ncpserve",
		CksumAlgorithm:    "md5",
	}

	data, err := json.Marshal(sc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var sc2 ServerConfig
	if err := json.Unmarshal(data, &sc2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if sc2.ProgramLogLevel != "debug" {
		t.Errorf("ProgramLogLevel = %q, want %q", sc2.ProgramLogLevel, "debug")
	}
	if sc2.ProgressStorePath != "/tmp/ncpserve" {
		t.Errorf("ProgressStorePath = %q, want %q", sc2.ProgressStorePath, "/tmp/ncpserve")
	}
	if !sc2.FileLogEnabled {
		t.Error("FileLogEnabled = false, want true")
	}
	if sc2.FileLogInterval != 10 {
		t.Errorf("FileLogInterval = %d, want 10", sc2.FileLogInterval)
	}
	if sc2.CksumAlgorithm != "md5" {
		t.Errorf("CksumAlgorithm = %q, want %q", sc2.CksumAlgorithm, "md5")
	}
}

func TestServerConfigEmptyRoundtrip(t *testing.T) {
	sc := ServerConfig{}
	data, err := json.Marshal(sc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var sc2 ServerConfig
	if err := json.Unmarshal(data, &sc2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if sc2.ProgramLogLevel != "" {
		t.Errorf("ProgramLogLevel = %q, want empty", sc2.ProgramLogLevel)
	}
}
