package parser

func Parse(line string) (string, []string) {
	argv := []string{}
	current := ""
	inQuotes := false

	for _, char := range line {
		switch char {
		case ' ', '\n', '\t':
			if inQuotes {
				current += string(char)
			} else if current != "" {
				argv = append(argv, current)
				current = ""
			}
		case '"':
			if inQuotes {
				argv = append(argv, current)
				current = ""
			}
			inQuotes = !inQuotes
		default:
			current += string(char)
		}
	}
	if current != "" {
		argv = append(argv, current)
	}
	
	if len(argv) < 2 {
		return "", []string{}
	}
	return argv[0], argv[1:]
}
