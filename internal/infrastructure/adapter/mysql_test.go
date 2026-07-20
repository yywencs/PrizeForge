package adapter

import "testing"

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
