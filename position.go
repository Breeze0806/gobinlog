package gbinlog

// Position 指定binlog的位置，以文件名和位移
type Position struct {
	Filename string `json:"filename"` //binlog文件名
	Offset   int64  `json:"offset"`   //在binlog文件中的位移
}

// IsZero means Position is existed
func (p Position) IsZero() bool {
	return p.Filename == "" || p.Offset == 0
}
