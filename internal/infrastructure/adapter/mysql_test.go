package adapter

import (
	"fmt"
	"hash/crc32"
	"testing"

	"gorm.io/gorm"
)

// TestResolveDatabaseDSN 验证主库和分库的 DSN 模板只替换第一个数据库后缀占位符。
func TestResolveDatabaseDSN(t *testing.T) {
	tests := []struct {
		name   string
		dsn    string
		suffix string
		want   string
	}{
		{
			name:   "default database",
			dsn:    "root:password@tcp(mysql:3306)/prizeforge%s?parseTime=True",
			suffix: "",
			want:   "root:password@tcp(mysql:3306)/prizeforge?parseTime=True",
		},
		{
			name:   "database shard",
			dsn:    "root:password@tcp(mysql:3306)/prizeforge%s?parseTime=True",
			suffix: "_02",
			want:   "root:password@tcp(mysql:3306)/prizeforge_02?parseTime=True",
		},
		{
			name:   "dsn without template",
			dsn:    "root:p%40ssword@tcp(mysql:3306)/prizeforge?parseTime=True",
			suffix: "_01",
			want:   "root:p%40ssword@tcp(mysql:3306)/prizeforge?parseTime=True",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveDatabaseDSN(tt.dsn, tt.suffix); got != tt.want {
				t.Fatalf("resolveDatabaseDSN() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDBStrategyUsesLocalTableIndexWithinEachDatabase 验证两库四表的全部八个哈希槽位
// 都会映射到目标分库内的 000～003 表，而不会在第二个分库生成不存在的 004～007 后缀。
func TestDBStrategyUsesLocalTableIndexWithinEachDatabase(t *testing.T) {
	db01 := &gorm.DB{}
	db02 := &gorm.DB{}
	router := &DBRouter{
		dbMap: map[string]*gorm.DB{
			"01": db01,
			"02": db02,
		},
		dbCount: 2,
		tbCount: 4,
	}
	dbs := []*gorm.DB{db01, db02}

	keysBySlot := findShardKeysBySlot(t, router.dbCount*router.tbCount)
	for slot, shardKey := range keysBySlot {
		gotDB, gotTableSuffix := router.DBStrategy(shardKey)
		wantDB := dbs[slot/router.tbCount]
		wantTableSuffix := fmt.Sprintf("%03d", slot%router.tbCount)

		if gotDB != wantDB || gotTableSuffix != wantTableSuffix {
			t.Fatalf("DBStrategy(%q) slot %d = (%p, %q), want (%p, %q)",
				shardKey, slot, gotDB, gotTableSuffix, wantDB, wantTableSuffix)
		}
	}
}

func findShardKeysBySlot(t *testing.T, slotCount int) []string {
	t.Helper()
	keys := make([]string, slotCount)
	found := 0
	for candidate := 0; candidate < 10000 && found < slotCount; candidate++ {
		shardKey := fmt.Sprintf("router-test-user-%d", candidate)
		slot := int(crc32.ChecksumIEEE([]byte(shardKey))) % slotCount
		if keys[slot] == "" {
			keys[slot] = shardKey
			found++
		}
	}
	if found != slotCount {
		t.Fatalf("found shard keys for %d/%d slots", found, slotCount)
	}
	return keys
}
