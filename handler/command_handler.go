package handler

import "fmt"

func handleCommand(cmd string, args []string) string {
	return fmt.Sprintf("Executing command %v with args %v\n", cmd, args)
}
