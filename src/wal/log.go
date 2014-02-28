package wal

import (
	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	"configuration"
	"fmt"
	"io"
	"os"
	"path"
	"protocol"
	"strconv"
	"strings"
)

type log struct {
	closed                 bool
	fileSize               int64
	file                   *os.File
	requestsSinceLastFlush int
	config                 *configuration.Configuration
	cachedSuffix           int
}

func newLog(file *os.File, config *configuration.Configuration) (*log, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	suffixString := strings.TrimLeft(path.Base(file.Name()), "log.")
	suffix, err := strconv.Atoi(suffixString)
	if err != nil {
		return nil, err
	}

	l := &log{
		file:         file,
		fileSize:     size,
		closed:       false,
		config:       config,
		cachedSuffix: suffix,
	}

	// if err := l.recover(); err != nil {
	// 	logger.Error("Error while recovering from file %s", file.Name())
	// 	return nil, err
	// }
	return l, nil
}

func (self *log) suffix() int {
	return self.cachedSuffix
}

func (self *log) internalFlush() error {
	logger.Debug("Fsyncing the log file to disk")
	return self.file.Sync()
}

func (self *log) close() error {
	if self.closed {
		return nil
	}
	self.internalFlush()
	return self.file.Close()
}

// func (self *log) recover() error {
// 	dir := filepath.Dir(self.file.Name())
// 	bookmarkPath := filepath.Join(dir, fmt.Sprintf("bookmark.%d", self.suffix()))
// 	_, err := os.Stat(bookmarkPath)
// 	if os.IsNotExist(err) {
// 		return nil
// 	}

// 	if err != nil {
// 		return err
// 	}

// 	// read the state from the bookmark file
// 	bookmark, err := os.OpenFile(bookmarkPath, os.O_RDONLY, 0)
// 	if err != nil {
// 		return err
// 	}
// 	defer bookmark.Close()
// 	if err := self.state.read(bookmark); err != nil {
// 		return err
// 	}

// 	logger.Debug("Recovering from previous state from file offset: %d", self.state.FileOffset)

// 	// replay the rest of the wal
// 	if _, err := self.file.Seek(self.state.FileOffset, os.SEEK_SET); err != nil {
// 		logger.Error("Cannot seek to %d. Error: %s", self.state.FileOffset, err)
// 		return err
// 	}

// 	replayChan := make(chan *replayRequest, 10)
// 	stopChan := make(chan struct{})

// 	go func() {
// 		self.replayFromFileLocation(self.file, map[uint32]struct{}{}, replayChan, stopChan)
// 	}()

// 	for {
// 		x := <-replayChan
// 		if x == nil {
// 			break
// 		}

// 		if x.err != nil {
// 			return x.err
// 		}

// 		self.state.recover(x)
// 		self.conditionalBookmarkAndIndex()
// 	}

// 	info, err := self.file.Stat()
// 	if err != nil {
// 		return err
// 	}
// 	self.state.setFileOffset(info.Size())

// 	return nil
// }

// func (self *log) assignSequenceNumbers(shardId uint32, request *protocol.Request) {
// 	if request.Series == nil {
// 		return
// 	}
// 	sequenceNumber := self.state.getCurrentSequenceNumber(shardId)
// 	for _, p := range request.Series.Points {
// 		if p.SequenceNumber != nil {
// 			continue
// 		}
// 		sequenceNumber++
// 		p.SequenceNumber = proto.Uint64(sequenceNumber)
// 	}
// 	self.state.setCurrentSequenceNumber(shardId, sequenceNumber)
// }

func (self *log) appendRequest(request *protocol.Request, shardId uint32) error {
	bytes, err := request.Encode()

	if err != nil {
		return err
	}
	// every request is preceded with the length, shard id and the request number
	hdr := &entryHeader{
		shardId:       shardId,
		requestNumber: request.GetRequestNumber(),
		length:        uint32(len(bytes)),
	}
	writtenHdrBytes, err := hdr.Write(self.file)
	if err != nil {
		logger.Error("Error while writing header: %s", err)
		return err
	}
	written, err := self.file.Write(bytes)
	if err != nil {
		logger.Error("Error while writing request: %s", err)
		return err
	}
	if written < len(bytes) {
		err = fmt.Errorf("Couldn't write entire request")
		logger.Error("Error while writing request: %s", err)
		return err
	}
	self.fileSize += int64(writtenHdrBytes + written)
	return nil
}

func (self *log) dupLogFile() (*os.File, error) {
	return os.OpenFile(self.file.Name(), os.O_RDONLY, 0)
}

func (self *log) getNextHeader(file *os.File) (int, *entryHeader, error) {
	hdr := &entryHeader{}
	numberOfBytes, err := hdr.Read(file)
	if err == io.EOF {
		return 0, nil, nil
	}
	return numberOfBytes, hdr, err
}

func (self *log) skipRequest(file *os.File, hdr *entryHeader) (err error) {
	_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
	return
}

func (self *log) skipToRequest(file *os.File, requestNumber uint32, order RequestNumberOrder) error {
	for {
		n, hdr, err := self.getNextHeader(file)
		if n == 0 {
			// EOF
			return nil
		}
		if err != nil {
			return err
		}
		if order.isBefore(hdr.requestNumber, requestNumber) {
			if err := self.skipRequest(file, hdr); err != nil {
				return err
			}
			continue
		}
		// seek back to the beginning of the request header
		_, err = file.Seek(int64(-n), os.SEEK_CUR)
		return err
	}
}

func (self *log) replayFromFileLocation(shardIds []uint32, offset int64) (chan *replayRequest, chan struct{}) {
	shardIdsSet := map[uint32]struct{}{}
	for _, shardId := range shardIds {
		shardIdsSet[shardId] = struct{}{}
	}
	replayChan := make(chan *replayRequest, 10)
	stopChan := make(chan struct{}, 1)
	go self.replay(shardIdsSet, offset, replayChan, stopChan)
	return replayChan, stopChan
}

func (self *log) replay(shardIdsSet map[uint32]struct{},
	offset int64,
	replayChan chan *replayRequest,
	stopChan chan struct{}) {

	defer func() { close(replayChan) }()

	file, err := self.dupLogFile()
	if err != nil {
		sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
		return
	}

	for {
		numberOfBytes, hdr, err := self.getNextHeader(file)
		if numberOfBytes == 0 {
			break
		}

		if err != nil {
			// TODO: the following line is all over the place. DRY
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		ok := false
		if len(shardIdsSet) == 0 {
			ok = true
		} else {
			_, ok = shardIdsSet[hdr.shardId]
		}
		if !ok {
			err = self.skipRequest(file, hdr)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
				return
			}
			continue
		}

		bytes := make([]byte, hdr.length)
		read, err := file.Read(bytes)
		if err == io.EOF {
			// file ends prematurely, truncate to the previous request
			logger.Warn("%s ends prematurely, truncating to last known good request", file.Name())
			offset, err := file.Seek(int64(-numberOfBytes), os.SEEK_CUR)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
				return
			}
			err = file.Truncate(offset)
			if err != nil {
				sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			}
			return
		}
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		if uint32(read) != hdr.length {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}
		req := &protocol.Request{}
		err = req.Decode(bytes)
		if err != nil {
			sendOrStop(newErrorReplayRequest(err), replayChan, stopChan)
			return
		}

		req.RequestNumber = proto.Uint32(hdr.requestNumber)
		replayRequest := &replayRequest{hdr.requestNumber, req, hdr.shardId, nil}
		if sendOrStop(replayRequest, replayChan, stopChan) {
			return
		}
	}
}

func sendOrStop(req *replayRequest, replayChan chan *replayRequest, stopChan chan struct{}) bool {
	if req.err != nil {
		logger.Error("Error in replay: %s", req.err)
	}

	select {
	case replayChan <- req:
	case _, ok := <-stopChan:
		logger.Debug("Stopping replay")
		return ok
	}
	return false
}

func (self *log) delete() {
	filePath := path.Join(self.config.WalDir, fmt.Sprintf("bookmark.%d", self.suffix()))
	os.Remove(filePath)
	filePath = path.Join(self.config.WalDir, fmt.Sprintf("log.%d", self.suffix()))
	os.Remove(filePath)
}
