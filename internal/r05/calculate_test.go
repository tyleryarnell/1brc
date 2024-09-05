package five

import "testing"

func Test_parseRow(t *testing.T) {
	type args struct {
		row []byte
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 float64
	}{
		{
			name:  "positive",
			args:  args{row: []byte("Bamako;36.9")},
			want:  "Bamako",
			want1: 36.9,
		},
		{
			name:  "negative",
			args:  args{row: []byte("Ulaanbaatar;-1.3")},
			want:  "Ulaanbaatar",
			want1: -1.3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := parseRow(tt.args.row)
			if got != tt.want {
				t.Errorf("parseRow() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("parseRow() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
