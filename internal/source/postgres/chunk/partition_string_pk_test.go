package chunk

import "testing"

// A varchar(n) primary key is introspected by its base name, "character
// varying"; the partitioner must treat that as a string type (issue #561).
func TestVarcharPrimaryKeyCanPartition(t *testing.T) {
	if !isStringType("character varying") {
		t.Error("isStringType(character varying) must be true so a varchar(n) PK can partition")
	}
}
