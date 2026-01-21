package topic

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"pxmq/client"
	"pxmq/message"
	"sync"
)

type Topic struct {
	Name        string
	messages    []message.Message
	subscribers map[*client.Subscriber]bool
	mu          sync.RWMutex
	cv          *sync.Cond
	walFile     *os.File
	nextMsgID   uint64
	walPath     string
}

func NewTopic(name string, dataDir string) *Topic {
	walPath := filepath.Join(dataDir, name+".wal")
	t := &Topic{
		Name:        name,
		messages:    make([]message.Message, 0),
		subscribers: make(map[*client.Subscriber]bool),
		walPath:     walPath,
		nextMsgID:   1,
	}
	t.cv = sync.NewCond(&t.mu)

	t.loadWAL()

	var err error
	t.walFile, err = os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open WAL file for topic %s: %v", name, err))
	}

	return t
}

func (t *Topic) Publish(payload []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg := message.NewMessage(t.nextMsgID, payload)
	t.nextMsgID++

	if err := t.appendToWAL(msg); err != nil {
		panic(fmt.Sprintf("Failed to append to WAL: %v", err))
	}

	t.messages = append(t.messages, *msg)
	t.cv.Broadcast()
}

func (t *Topic) Consume(sub *client.Subscriber, replay bool) {
	t.mu.Lock()

	var startOffset int
	if replay {
		lastAckedID := sub.LastAckedID[t.Name]
		startOffset = len(t.messages)
		for i, msg := range t.messages {
			if msg.ID > lastAckedID {
				startOffset = i
				break
			}
		}
	} else {
		startOffset = len(t.messages)
	}

	sub.Offsets[t.Name] = startOffset

	for sub.Active.Load() {
		for sub.Offsets[t.Name] >= len(t.messages) {
			t.cv.Wait()
		}

		msg := t.messages[sub.Offsets[t.Name]]
		sub.Offsets[t.Name]++

		data := fmt.Sprintf("MSG %s %d %d\n%s", t.Name, msg.ID, len(msg.Payload), msg.Payload)

		t.mu.Unlock()
		sub.MessageCh <- []byte(data)
		t.mu.Lock()
		if _, exists := t.subscribers[sub]; !exists {
			t.mu.Unlock()
			return
		}
	}
	t.mu.Unlock()
}

func (t *Topic) AddSubscriber(sub *client.Subscriber) {
	t.mu.Lock()
	t.subscribers[sub] = true
	t.mu.Unlock()
}

func (t *Topic) Unsubscribe(sub *client.Subscriber) {
	t.mu.Lock()
	delete(t.subscribers, sub)
	t.cv.Broadcast()
	t.mu.Unlock()
}

func (t *Topic) HasSubscriber(sub *client.Subscriber) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.subscribers[sub]
	return exists
}

func (t *Topic) loadWAL() {
	file, err := os.Open(t.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(fmt.Sprintf("Failed to open WAL file for topic %s: %v", t.Name, err))
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		var msgID uint64
		if err := binary.Read(reader, binary.BigEndian, &msgID); err != nil {
			if err == io.EOF {
				break
			}
			panic(fmt.Sprintf("Failed to read msgID from WAL for topic %s: %v", t.Name, err))
		}

		var payloadLen uint32
		if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
			panic(fmt.Sprintf("Failed to read payload length from WAL for topic %s: %v", t.Name, err))
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			panic(fmt.Sprintf("Failed to read payload from WAL for topic %s: %v", t.Name, err))
		}

		msg := message.NewMessage(msgID, payload)
		t.messages = append(t.messages, *msg)

		if msgID >= t.nextMsgID {
			t.nextMsgID = msgID + 1
		}
	}
}

func (t *Topic) appendToWAL(msg *message.Message) error {
	if err := binary.Write(t.walFile, binary.BigEndian, msg.ID); err != nil {
		return err
	}
	payloadLen := uint32(len(msg.Payload))
	if err := binary.Write(t.walFile, binary.BigEndian, payloadLen); err != nil {
		return err
	}
	if _, err := t.walFile.Write(msg.Payload); err != nil {
		return err
	}
	return t.walFile.Sync()
}

func (t *Topic) Close() error {
	if t.walFile != nil {
		return t.walFile.Close()
	}
	return nil
}
