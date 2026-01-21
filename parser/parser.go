package parser

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Command struct {
	Type    string
	Args    []string
	Payload []byte
}

func ParseCommand(r *bufio.Reader) (*Command, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty line")
	}

	parts := strings.Split(line, " ")
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid command")
	}

	cmd := &Command{
		Type: strings.ToUpper(parts[0]),
	}

	switch cmd.Type {
	case "PUB":
		if len(parts) < 3 {
			return nil, fmt.Errorf("PUB requires topic and length")
		}
		cmd.Args = []string{parts[1]}
		length, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid length: %v", err)
		}
		payload := make([]byte, length)
		_, err = io.ReadFull(r, payload)
		if err != nil {
			return nil, err
		}
		cmd.Payload = payload

	case "SUB", "UNSUB":
		if len(parts) < 2 {
			return nil, fmt.Errorf("%s requires topics", cmd.Type)
		}

		if cmd.Type == "SUB" && parts[len(parts)-1] == "*" {
			cmd.Args = parts[1 : len(parts)-1]
			cmd.Args = append(cmd.Args, "*")
		} else {
			cmd.Args = parts[1:]
		}

	default:
		return nil, fmt.Errorf("unknown command: %s", cmd.Type)
	}

	return cmd, nil
}
