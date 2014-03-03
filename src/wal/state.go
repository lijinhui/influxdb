package wal

import (
	"configuration"
	"encoding/gob"
	"io"
	"protocol"
)

const (
	CURRENT_VERSION = 1
)

type state struct {
	Version               byte
	FileOffset            int64 // the file offset at which this bookmark was created
	Index                 *index
	LargestRequestNumber  uint32 // largest request number in the log file
	TotalNumberOfRequests int    // number of requests in this log file

	// configuration
	config *configuration.Configuration
}

func newState(config *configuration.Configuration) *state {
	return &state{
		Version: CURRENT_VERSION,
		Index: &index{
			Entries: make([]*indexEntry, 0),
		},
	}
}

func (self *state) update(request *protocol.Request, fileOffset uint64) {
	if self.LargestRequestNumber < request.GetRequestNumber() {
		self.LargestRequestNumber = request.GetRequestNumber()
	}
	self.TotalNumberOfRequests++

	if self.TotalNumberOfRequests%self.config.WalIndexAfterRequests == 0 {
		firstRequestNumber := self.LargestRequestNumber - uint32(self.config.WalIndexAfterRequests)
		self.Index.addEntry(firstRequestNumber, uint32(self.config.WalIndexAfterRequests), fileOffset)
	}

	if self.TotalNumberOfRequests%self.config.WalFlushAfterRequests == 0 {

	}
}

func (self *state) setFileOffset(offset int64) {
	self.FileOffset = offset
}

func (self *state) write(w io.Writer) error {
	return gob.NewEncoder(w).Encode(self)
}

func (self *state) read(r io.Reader) error {
	return gob.NewDecoder(r).Decode(self)
}
