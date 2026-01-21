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
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Topic struct {
	Name            string
	messages        []message.Message
	subscribers     map[*client.Subscriber]bool
	mu              sync.RWMutex
	cv              *sync.Cond
	activeSegment   *os.File
	nextMsgID       uint64
	dataDir         string
	activeSegmentID uint64
	maxSegmentSize  int64
}

func NewTopic(name string, dataDir string) *Topic {
	t := &Topic{
		Name:           name,
		messages:       make([]message.Message, 0),
		subscribers:    make(map[*client.Subscriber]bool),
		dataDir:        dataDir,
		nextMsgID:      1,
		maxSegmentSize: 64 * 1024 * 1024,
	}
	t.cv = sync.NewCond(&t.mu)

	topicDir := filepath.Join(dataDir, name)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create topic directory for %s: %v", name, err))
	}

	t.loadSegments()

	t.openActiveSegment()

	go func() {
		ticker := time.NewTicker(60 * 60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.Prune()
			}
		}
	}()

	return t
}

func (t *Topic) Publish(payload []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg := message.NewMessage(t.nextMsgID, payload)
	t.nextMsgID++

	if err := t.appendToActiveSegment(msg); err != nil {
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
	defer t.mu.Unlock()

	t.subscribers[sub] = true
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

func (t *Topic) loadSegments() {
	topicDir := filepath.Join(t.dataDir, t.Name)
	pattern := filepath.Join(topicDir, "segment_*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		panic(fmt.Sprintf("Failed to glob segment files for topic %s: %v", t.Name, err))
	}

	if len(matches) == 0 {
		t.activeSegmentID = 1
		return
	}

	var segmentIDs []uint64
	segmentRegex := regexp.MustCompile(`segment_(\d+)$`)
	for _, match := range matches {
		if matches := segmentRegex.FindStringSubmatch(filepath.Base(match)); len(matches) == 2 {
			if id, err := strconv.ParseUint(matches[1], 10, 64); err == nil {
				segmentIDs = append(segmentIDs, id)
			}
		}
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })

	for _, segmentID := range segmentIDs {
		segmentPath := filepath.Join(topicDir, fmt.Sprintf("segment_%d", segmentID))
		t.loadSegment(segmentPath)
	}

	t.activeSegmentID = segmentIDs[len(segmentIDs)-1]
}

func (t *Topic) loadSegment(segmentPath string) {
	file, err := os.Open(segmentPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to open segment file %s: %v", segmentPath, err))
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		var msgID uint64
		if err := binary.Read(reader, binary.BigEndian, &msgID); err != nil {
			if err == io.EOF {
				break
			}
			panic(fmt.Sprintf("Failed to read msgID from segment %s: %v", segmentPath, err))
		}

		var payloadLen uint32
		if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
			panic(fmt.Sprintf("Failed to read payload length from segment %s: %v", segmentPath, err))
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			panic(fmt.Sprintf("Failed to read payload from segment %s: %v", segmentPath, err))
		}

		msg := message.NewMessage(msgID, payload)
		t.messages = append(t.messages, *msg)

		if msgID >= t.nextMsgID {
			t.nextMsgID = msgID + 1
		}
	}
}

func (t *Topic) openActiveSegment() {
	topicDir := filepath.Join(t.dataDir, t.Name)
	segmentPath := filepath.Join(topicDir, fmt.Sprintf("segment_%d", t.activeSegmentID))
	var err error
	t.activeSegment, err = os.OpenFile(segmentPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open active segment for topic %s: %v", t.Name, err))
	}
}

func (t *Topic) appendToActiveSegment(msg *message.Message) error {
	if t.shouldRotateSegment() {
		if err := t.rotateSegment(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	if err := binary.Write(t.activeSegment, binary.BigEndian, msg.ID); err != nil {
		return err
	}
	payloadLen := uint32(len(msg.Payload))
	if err := binary.Write(t.activeSegment, binary.BigEndian, payloadLen); err != nil {
		return err
	}
	if _, err := t.activeSegment.Write(msg.Payload); err != nil {
		return err
	}
	return t.activeSegment.Sync()
}

func (t *Topic) shouldRotateSegment() bool {
	if t.activeSegment == nil {
		return false
	}

	stat, err := t.activeSegment.Stat()
	if err != nil {
		return false
	}

	return stat.Size() >= t.maxSegmentSize
}

func (t *Topic) rotateSegment() error {
	if t.activeSegment != nil {
		if err := t.activeSegment.Close(); err != nil {
			return err
		}
	}

	t.activeSegmentID++

	t.openActiveSegment()

	return nil
}

func (t *Topic) Close() error {
	if t.activeSegment != nil {
		return t.activeSegment.Close()
	}
	return nil
}

func (t *Topic) Prune() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	minAckedID := uint64(0)
	first := true
	for sub := range t.subscribers {
		if ackedID, exists := sub.LastAckedID[t.Name]; exists {
			if first || ackedID < minAckedID {
				minAckedID = ackedID
				first = false
			}
		}
	}
	if first {
		return nil
	}

	keepIndex := 0
	for i, msg := range t.messages {
		if msg.ID > minAckedID {
			keepIndex = i
			break
		}
	}

	if keepIndex == 0 && len(t.messages) > 0 && t.messages[0].ID <= minAckedID {
		keepIndex = len(t.messages)
	}

	if keepIndex == 0 {
		return nil
	}

	if t.activeSegment != nil {
		t.activeSegment.Close()
		t.activeSegment = nil
	}

	topicDir := filepath.Join(t.dataDir, t.Name)
	pattern := filepath.Join(topicDir, "segment_*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			return err
		}
	}

	t.activeSegmentID = 1
	t.openActiveSegment()

	for i := keepIndex; i < len(t.messages); i++ {
		msg := &t.messages[i]
		if err := t.appendToActiveSegment(msg); err != nil {
			return err
		}
	}

	t.messages = t.messages[keepIndex:]

	return nil
}
