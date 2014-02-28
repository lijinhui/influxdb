package wal

import (
	"encoding/gob"
	"io"
	"math"
)

type logFileState struct {
	Name               string
	FirstRequestNumber uint32
	LastRequestNumber  uint32
	Index              *index
}

type globalState struct {
	Version                   int
	CurrentFile               string
	FileOffset                int64 // the file offset at which the state was written
	RequestsSinceLastBookmark int
	RequestsSinceLastIndex    int
	RequestsSinceLastFlush    int
	ShardLastSequenceNumber   map[uint32]uint64
	ServerLastRequestNumber   map[uint32]uint32
	FirstRequestNumber        uint32
	LargestRequestNumber      uint32
	Files                     []*logFileState
}

func (self *globalState) isAfter(left, right uint32) bool {
	if left == right {
		return false
	}
	if left >= self.FirstRequestNumber && right >= self.FirstRequestNumber {
		return left > right
	}
	if left <= self.LargestRequestNumber && right <= self.LargestRequestNumber {
		return left > right
	}
	return left <= self.LargestRequestNumber
}

func (self *globalState) isAfterOrEqual(left, right uint32) bool {
	return left == right || self.isAfter(left, right)
}

func (self *globalState) isBefore(left, right uint32) bool {
	return !self.isAfterOrEqual(left, right)
}

func (self *globalState) isBeforeOrEqual(left, right uint32) bool {
	return !self.isAfter(left, right)
}

func (self *globalState) recover(replay *replayRequest) {
	if self.LargestRequestNumber < replay.requestNumber {
		self.LargestRequestNumber = replay.requestNumber
	}

	lastSequenceNumber := self.ShardLastSequenceNumber[replay.shardId]

	for _, p := range replay.request.Series.Points {
		if seq := p.GetSequenceNumber(); seq > lastSequenceNumber {
			lastSequenceNumber = seq
		}
	}

	self.ShardLastSequenceNumber[replay.shardId] = lastSequenceNumber
}

func (self *globalState) getNextRequestNumber() uint32 {
	self.LargestRequestNumber++
	return self.LargestRequestNumber
}

func (self *globalState) getCurrentSequenceNumber(shardId uint32) uint64 {
	return self.ShardLastSequenceNumber[shardId]
}

func (self *globalState) setCurrentSequenceNumber(shardId uint32, sequenceNumber uint64) {
	self.ShardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *globalState) commitRequestNumber(serverId, requestNumber uint32) {
	self.ServerLastRequestNumber[serverId] = requestNumber
}

func (self *globalState) LowestCommitedRequestNumber() uint32 {
	requestNumber := uint32(math.MaxUint32)
	for _, number := range self.ServerLastRequestNumber {
		if number < requestNumber {
			requestNumber = number
		}
	}
	return requestNumber
}

func (self *globalState) write(w io.Writer) error {
	return gob.NewEncoder(w).Encode(self)
}

func (self *globalState) read(r io.Reader) error {
	return gob.NewDecoder(r).Decode(self)
}

func (self *globalState) LastLogFile() *logFileState {
	return self.Files[len(self.Files)-1]
}

func (self *globalState) FirstLogFile() *logFileState {
	return self.Files[0]
}
