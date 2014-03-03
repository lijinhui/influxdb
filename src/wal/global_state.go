package wal

import (
	"encoding/gob"
	"io"
	"math"
)

type GlobalState struct {
	// global state
	Version                   byte
	RequestsSinceLastBookmark int
	FirstRequestNumber        uint32
	LargestRequestNumber      uint32
	ShardLastSequenceNumber   map[uint32]uint64
	ServerLastRequestNumber   map[uint32]uint32
}

func (self *GlobalState) write(w io.Writer) error {
	return gob.NewEncoder(w).Encode(self)
}

func (self *GlobalState) read(r io.Reader) error {
	return gob.NewDecoder(r).Decode(self)
}

func (self *GlobalState) recover(replay *replayRequest) {
	lastSequenceNumber := self.ShardLastSequenceNumber[replay.shardId]

	for _, p := range replay.request.Series.Points {
		if seq := p.GetSequenceNumber(); seq > lastSequenceNumber {
			lastSequenceNumber = seq
		}
	}

	self.ShardLastSequenceNumber[replay.shardId] = lastSequenceNumber
}

func (self *GlobalState) getNextRequestNumber() uint32 {
	self.LargestRequestNumber++
	return self.LargestRequestNumber
}

func (self *GlobalState) getCurrentSequenceNumber(shardId uint32) uint64 {
	return self.ShardLastSequenceNumber[shardId]
}

func (self *GlobalState) setCurrentSequenceNumber(shardId uint32, sequenceNumber uint64) {
	self.ShardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *GlobalState) commitRequestNumber(serverId, requestNumber uint32) {
	self.ServerLastRequestNumber[serverId] = requestNumber
}

func (self *GlobalState) LowestCommitedRequestNumber() uint32 {
	requestNumber := uint32(math.MaxUint32)
	for _, number := range self.ServerLastRequestNumber {
		if number < requestNumber {
			requestNumber = number
		}
	}
	return requestNumber
}

func (self *GlobalState) isAfter(left, right uint32) bool {
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

func (self *GlobalState) isAfterOrEqual(left, right uint32) bool {
	return left == right || self.isAfter(left, right)
}

func (self *GlobalState) isBefore(left, right uint32) bool {
	return !self.isAfterOrEqual(left, right)
}

func (self *GlobalState) isBeforeOrEqual(left, right uint32) bool {
	return !self.isAfter(left, right)
}
