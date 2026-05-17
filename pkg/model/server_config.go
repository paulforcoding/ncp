package model

// ServerConfig is a curated subset of client config sent to the server
// via MsgInit.ConfigJSON. It excludes sensitive fields like Profiles.
type ServerConfig struct {
	ProgramLogLevel   string `json:"ProgramLogLevel"`
	ProgramLogOutput  string `json:"ProgramLogOutput"`
	FileLogEnabled    bool   `json:"FileLogEnabled"`
	FileLogOutput     string `json:"FileLogOutput"`
	FileLogInterval   int    `json:"FileLogInterval"`
	ProgressStorePath string `json:"ProgressStorePath"`
	CksumAlgorithm    string `json:"CksumAlgorithm"`
}
