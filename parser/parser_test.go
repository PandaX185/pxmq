package parser

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input        string
		expectedCmd  string
		expectedArgs []string
	}{
		{"pub topic message", "pub", []string{"topic", "message"}},
		{"sub topic", "sub", []string{"topic"}},
		{"unsub topic", "unsub", []string{"topic"}},
		{"pub topic \"message with spaces\"", "pub", []string{"topic", "message with spaces"}},
		{"sub topic1 topic2 *", "sub", []string{"topic1", "topic2", "*"}},
		{"", "", []string{}},
		{"cmd", "", []string{}},
	}

	for _, test := range tests {
		cmd, args := Parse(test.input)
		if cmd != test.expectedCmd {
			t.Errorf("For input %q, expected cmd %q, got %q", test.input, test.expectedCmd, cmd)
		}
		if !reflect.DeepEqual(args, test.expectedArgs) {
			t.Errorf("For input %q, expected args %v, got %v", test.input, test.expectedArgs, args)
		}
	}
}
