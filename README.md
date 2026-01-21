# pxmq

A lightweight, TCP-based publish-subscribe message queue system written in Go.

## Features

- **Publish/Subscribe Pattern**: Topics maintain lists of subscribers with automatic fan-out
- **TCP Protocol**: Simple command-based protocol over TCP connections
- **Message Persistence**: Topics store message history for replay
- **Concurrent**: Thread-safe operations with proper synchronization
- **Simple API**: Easy-to-use commands for publishing and subscribing

## Installation

```bash
git clone https://github.com/yourusername/pxmq.git
cd pxmq
go build
```

## Usage

Start the server:

```bash
./pxmq
```

The server listens on port 8888 by default.

## Protocol

Connect to the server using any TCP client (telnet, netcat, etc.):

```bash
telnet localhost 8888
```

### Commands

- `pub <topic> <message>` - Publish a message to a topic
- `sub <topic>` - Subscribe to a topic (receives new messages)
- `sub <topic> *` - Subscribe to a topic with replay (receives old messages too)
- `unsub <topic>` - Unsubscribe from a topic

### Responses

- `OK` - Command successful
- `MESSAGE <topic> <message>` - Incoming message
- `ERROR <message>` - Command failed

## Example

```bash
# Terminal 1: Start server
./pxmq

# Terminal 2: Connect subscriber
telnet localhost 8888
sub mytopic
OK

# Terminal 3: Connect publisher
telnet localhost 8888
pub mytopic "Hello World"
OK

# Terminal 2 will receive:
MESSAGE mytopic Hello World
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