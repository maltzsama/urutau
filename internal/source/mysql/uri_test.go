package mysql

import "testing"

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    *URI
		wantErr bool
	}{
		{
			name: "full uri",
			uri:  "mysql://root:secret@localhost:3306/mydb",
			want: &URI{User: "root", Password: "secret", Host: "localhost", Port: "3306", DB: "mydb"},
		},
		{
			name: "no port defaults to 3306",
			uri:  "mysql://root:secret@localhost/mydb",
			want: &URI{User: "root", Password: "secret", Host: "localhost", Port: "3306", DB: "mydb"},
		},
		{
			name: "no password",
			uri:  "mysql://root@localhost/mydb",
			want: &URI{User: "root", Password: "", Host: "localhost", Port: "3306", DB: "mydb"},
		},
		{
			name:    "wrong scheme",
			uri:     "postgres://root@localhost/mydb",
			wantErr: true,
		},
		{
			name:    "no host",
			uri:     "mysql://root@/mydb",
			wantErr: true,
		},
		{
			name:    "no db",
			uri:     "mysql://root@localhost/",
			wantErr: true,
		},
		{
			name:    "empty uri",
			uri:     "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseURI(%q) error = %v, wantErr %v", tt.uri, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.User != tt.want.User || got.Password != tt.want.Password || got.Host != tt.want.Host || got.Port != tt.want.Port || got.DB != tt.want.DB {
				t.Fatalf("ParseURI(%q) = %+v, want %+v", tt.uri, got, tt.want)
			}
		})
	}
}

func TestURIAddr(t *testing.T) {
	u := &URI{Host: "localhost", Port: "3306"}
	if got := u.Addr(); got != "localhost:3306" {
		t.Fatalf("Addr() = %q, want localhost:3306", got)
	}
}

func TestURIQueryDSN(t *testing.T) {
	u := &URI{User: "root", Password: "secret", Host: "localhost", Port: "3306", DB: "mydb"}
	want := "root:secret@tcp(localhost:3306)/mydb?parseTime=true"
	if got := u.QueryDSN(); got != want {
		t.Fatalf("QueryDSN() = %q, want %q", got, want)
	}
}
