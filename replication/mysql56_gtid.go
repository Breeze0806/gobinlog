/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replication

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const mysql56FlavorID = "MySQL56"

// parseMysql56GTID is registered as a GTID parser.
func parseMysql56GTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid MySQL 5.6 GTID (%v): expecting UUID:Sequence", s)
	}

	// Parse Server ID.
	sid, err := ParseSID(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid MySQL 5.6 GTID Server ID (%v) err: %v", parts[0], err)
	}

	// Parse Sequence number.
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid MySQL 5.6 GTID Sequence number (%v)  err: %v", parts[1], err)
	}

	return Mysql56GTID{Server: sid, Sequence: seq}, nil
}

// SID is the 16-byte unique ID of a MySQL 5.6 server.
type SID [16]byte

// String prints an SID in the form used by MySQL 5.6.
func (sid SID) String() string {
	dst := []byte("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	hex.Encode(dst, sid[:4])
	hex.Encode(dst[9:], sid[4:6])
	hex.Encode(dst[14:], sid[6:8])
	hex.Encode(dst[19:], sid[8:10])
	hex.Encode(dst[24:], sid[10:16])
	return string(dst)
}

// ParseSID parses an SID in the form used by MySQL 5.6.
func ParseSID(s string) (sid SID, err error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return sid, fmt.Errorf("invalid MySQL 5.6 SID %q", s)
	}

	// Drop the dashes so we can just check the error of Decode once.
	b := make([]byte, 0, 32)
	b = append(b, s[:8]...)
	b = append(b, s[9:13]...)
	b = append(b, s[14:18]...)
	b = append(b, s[19:23]...)
	b = append(b, s[24:]...)

	if _, err := hex.Decode(sid[:], b); err != nil {
		return sid, fmt.Errorf("invalid MySQL 5.6 SID %q err: %v", s, err)
	}
	return sid, nil
}

// Mysql56GTID implements GTID
type Mysql56GTID struct {
	// Server is the SID of the server that originally committed the transaction.
	Server SID
	// Sequence is the sequence number of the transaction within a given Server's
	// scope.
	Sequence int64
}

// String implements GTID.String().
func (m Mysql56GTID) String() string {
	return fmt.Sprintf("%s:%d", m.Server, m.Sequence)
}

// Flavor implements GTID.Flavor().
func (m Mysql56GTID) Flavor() string {
	return mysql56FlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (m Mysql56GTID) SequenceDomain() interface{} {
	return nil
}

// SourceServer implements GTID.SourceServer().
func (m Mysql56GTID) SourceServer() interface{} {
	return m.Server
}

// SequenceNumber implements GTID.SequenceNumber().
func (m Mysql56GTID) SequenceNumber() interface{} {
	return m.Sequence
}

// GTIDSet implements GTID.GTIDSet().
func (m Mysql56GTID) GTIDSet() GTIDSet {
	return Mysql56GTIDSet{}.AddGTID(m)
}

func init() {
	gtidParsers[mysql56FlavorID] = parseMysql56GTID
}
