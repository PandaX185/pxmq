# pxmq

A lightweight, TCP-based publish-subscribe message queue system written in Go.

## Features

- **Publish/Subscribe Pattern**: Topics maintain lists of subscribers with automatic fan-out
- **TCP Protocol**: Simple command-based protocol over TCP connections
- **Message Persistence**: Topics store message history for replay
- **Concurrent**: Thread-safe operations with proper synchronization
- **Simple API**: Easy-to-use commands for publishing and subscribing

## Installation

### From Source

```bash
git clone https://github.com/yourusername/pxmq.git
cd pxmq
go build
```

### Using Docker

```bash
git clone https://github.com/yourusername/pxmq.git
cd pxmq
docker build -t pxmq .
docker run -p 8888:8888 pxmq
```

To use a different port:

```bash
docker run -p 9999:9999 -e PORT=9999 pxmq
```

## Usage

### Running Locally

Start the server:

```bash
./pxmq
```

Or specify a custom port:

```bash
./pxmq -port 9999
```

### Running with Docker

```bash
docker run -p 8888:8888 pxmq
```

Or with custom port:

```bash
docker run -p 9999:9999 -e PORT=9999 pxmq
```

The server listens on the specified port (default: 8888).

## Protocol

pxmq uses a binary-safe TCP protocol with length-prefixed payloads.

### Commands

- `PUB <topic> <len>\n<payload>` - Publish a message to a topic
- `SUB <topics> [*]\n` - Subscribe to topics (use `*` for replay of old messages)
- `UNSUB <topics>\n` - Unsubscribe from topics

### Server Responses

- `+OK\n` - Command successful
- `-ERR <message>\n` - Command failed

### Server Push

- `MSG <topic> <len>\n<payload>` - Incoming message from subscribed topic

## Client Examples

Here are examples of how to connect to pxmq from different programming languages. Each example shows how to subscribe to a topic and publish a message. Since pxmq uses a simple TCP-based protocol, you can implement clients in any language that supports TCP sockets.

### Go Client

```go
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8888")
	if err != nil {
		fmt.Println("Error connecting:", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Send subscribe command
	fmt.Fprintf(conn, "SUB mytopic\n")

	// Read response
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		fmt.Println("Response:", scanner.Text())
	}

	// Send publish command with length-prefixed payload
	message := "Hello from Go!"
	fmt.Fprintf(conn, "PUB mytopic %d\n%s", len(message), message)

	// Read response
	if scanner.Scan() {
		fmt.Println("Response:", scanner.Text())
	}

	// Read messages
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MSG ") {
			// Parse MSG command
			parts := strings.Split(line, " ")
			if len(parts) >= 3 {
				topic := parts[1]
				// In real implementation, parse length and read payload
				fmt.Printf("Received message on topic %s\n", topic)
			}
		}
	}
}
```

### Node.js Client

```javascript
const net = require('net');

const client = net.createConnection({ port: 8888, host: 'localhost' }, () => {
  console.log('Connected to pxmq server');

  // Subscribe to a topic
  client.write('SUB mytopic\n');
});

client.on('data', (data) => {
  const message = data.toString();
  if (message.startsWith('+OK')) {
    console.log('Subscribed successfully');
    // Publish a message
    const payload = 'Hello from Node.js!';
    client.write(`PUB mytopic ${payload.length}\n${payload}`);
  } else if (message.startsWith('MSG ')) {
    console.log('Received message:', message.trim());
  } else if (message.startsWith('-ERR')) {
    console.error('Error:', message.trim());
  }
});

client.on('end', () => {
  console.log('Disconnected from server');
});

client.on('error', (err) => {
  console.error('Connection error:', err);
});
```

### Java Client

```java
import java.io.*;
import java.net.*;

public class PxmqClient {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("localhost", 8888);
            System.out.println("Connected to pxmq server");

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            // Subscribe to topic
            out.println("SUB mytopic");
            System.out.println("Response: " + in.readLine());

            // Publish a message with length prefix
            String message = "Hello from Java!";
            out.println("PUB mytopic " + message.length());
            out.println(message);
            System.out.println("Response: " + in.readLine());

            // Read messages
            String line;
            while ((line = in.readLine()) != null) {
                if (line.startsWith("MSG ")) {
                    // Parse MSG command - in real implementation, read the payload
                    System.out.println("Received message: " + line);
                } else {
                    System.out.println("Response: " + line);
                }
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
```

## Testing

Run the test suite:

```bash
go test ./...
```

Run with race detection:

```bash
go test -race ./...
```

## Architecture

- **Broker**: Manages topics and routing
- **Topic**: Maintains subscribers and message history
- **Subscriber**: Client connection with subscription management
- **Handler**: Processes commands and manages client connections

## License

MIT License - see LICENSE file for details