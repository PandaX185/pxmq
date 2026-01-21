package parser

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		input           string
		expectedType    string
		expectedArgs    []string
		expectedPayload []byte
		expectError     bool
	}{
		{"PUB topic 5\nhello", "PUB", []string{"topic"}, []byte("hello"), false},
		{"SUB topic1 topic2\n", "SUB", []string{"topic1", "topic2"}, nil, false},
		{"SUB topic *\n", "SUB", []string{"topic", "*"}, nil, false},
		{"UNSUB topic\n", "UNSUB", []string{"topic"}, nil, false},
		{"PUB topic\n", "", nil, nil, true},
		{"INVALID cmd\n", "", nil, nil, true},
	}

	for _, test := range tests {
		reader := bufio.NewReader(strings.NewReader(test.input))
		cmd, err := ParseCommand(reader)

		if test.expectError {
			if err == nil {
				t.Errorf("For input %q, expected error but got none", test.input)
			}
			continue
		}

		if err != nil {
			t.Errorf("For input %q, unexpected error: %v", test.input, err)
			continue
		}

		if cmd.Type != test.expectedType {
			t.Errorf("For input %q, expected type %q, got %q", test.input, test.expectedType, cmd.Type)
		}

		if !reflect.DeepEqual(cmd.Args, test.expectedArgs) {
			t.Errorf("For input %q, expected args %v, got %v", test.input, test.expectedArgs, cmd.Args)
		}

		if !reflect.DeepEqual(cmd.Payload, test.expectedPayload) {
			t.Errorf("For input %q, expected payload %v, got %v", test.input, test.expectedPayload, cmd.Payload)
		}
	}
}
