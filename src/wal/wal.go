package wal

import (
	logger "code.google.com/p/log4go"
	"configuration"
	"fmt"
	"os"
	"path"
	"protocol"
	"sort"
	"strings"
)

type WAL struct {
	config             *configuration.Configuration
	serverId           uint32
	nextLogFileSuffix  int
	requestsPerLogFile int
	entries            chan interface{}
	state              *globalState
	stateFile          *os.File
	logFiles           []*log
}

const HOST_ID_OFFSET = uint64(10000)

func NewWAL(config *configuration.Configuration) (*WAL, error) {
	if config.WalDir == "" {
		return nil, fmt.Errorf("wal directory cannot be empty")
	}
	_, err := os.Stat(config.WalDir)

	if os.IsNotExist(err) {
		err = os.MkdirAll(config.WalDir, 0755)
	}

	if err != nil {
		return nil, err
	}

	dir, err := os.Open(config.WalDir)
	if err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	nextLogFileSuffix := 0
	logFiles := make([]*log, 0)
	for _, name := range names {
		if !strings.HasPrefix(name, "log.") {
			continue
		}
		f, err := os.OpenFile(path.Join(config.WalDir, name), os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		logFile, err := newLog(f, config)
		if err != nil {
			return nil, err
		}
		if suffix := logFile.suffix(); suffix > nextLogFileSuffix {
			nextLogFileSuffix = suffix
		}
		logFiles = append(logFiles, logFile)
	}

	stateFile, err := os.OpenFile(path.Join("global"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	state := &globalState{}
	err = state.read(stateFile)
	if err != nil {
		return nil, err
	}

	// sort the logfiles by the first request number in the log
	wal := &WAL{
		config:             config,
		logFiles:           logFiles,
		requestsPerLogFile: config.WalRequestsPerLogFile,
		nextLogFileSuffix:  nextLogFileSuffix,
		entries:            make(chan interface{}, 10),
		state:              state,
		stateFile:          stateFile,
	}

	// if we don't have any log files open yet, open a new one
	if len(logFiles) == 0 {
		_, err = wal.createNewLog()
	} else {
		sort.Sort(sortableLogSlice{logFiles, state.Files, state})
	}

	go wal.processEntries()

	return wal, err
}

func (self *WAL) SetServerId(id uint32) {
	self.serverId = id
}

// Marks a given request for a given server as committed
func (self *WAL) Commit(requestNumber uint32, serverId uint32) error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &commitEntry{confirmationChan, serverId, requestNumber}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *WAL) RecoverServerFromLastCommit(serverId uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	requestNumber := self.state.ServerLastRequestNumber[serverId]
	return self.RecoverServerFromRequestNumber(requestNumber, shardIds, yield)
}

// In the case where this server is running and another one in the cluster stops responding, at some point this server will have to just write
// requests to disk. When the downed server comes back up, it's this server's responsibility to send out any writes that were queued up. If
// the yield function returns nil then the request is committed.
func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	var firstLogFile int

outer:
	for idx, logFile := range self.logFiles[firstLogFile:] {
		logFileState := self.state.Files[idx+firstLogFile]
		offset := logFileState.Index.requestOffset(self.state, requestNumber)
		logger.Info("Replaying from %s", logFile.file.Name())
		count := 0
		ch, stopChan := logFile.replayFromFileLocation(shardIds, int64(offset))
		for {
			x := <-ch
			if x == nil {
				logger.Info("%s yielded %d requests", logFile.file.Name(), count)
				continue outer
			}

			if x.err != nil {
				return x.err
			}

			if err := yield(x.request, x.shardId); err != nil {
				stopChan <- struct{}{}
				return err
			}
			count++
		}
		close(stopChan)
	}
	return nil
}

func (self *WAL) Close() error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &closeEntry{confirmationChan}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *WAL) processClose() error {
	for _, l := range self.logFiles {
		if err := l.close(); err != nil {
			return err
		}
	}
	return nil
}

// PRIVATE functions

func (self *WAL) processEntries() {
	for {
		e := <-self.entries
		switch x := e.(type) {
		case *commitEntry:
			self.processCommitEntry(x)
		case *appendEntry:
			self.processAppendEntry(x)
		case *closeEntry:
			x.confirmation <- &confirmation{0, self.processClose()}
			logger.Info("Closing wal")
			return
		default:
			panic(fmt.Errorf("unknown entry type %T", e))
		}
	}
}

func (self *WAL) processAppendEntry(e *appendEntry) {
	if self.shouldRotateTheLogFile() {
		if err := self.rotateTheLogFile(); err != nil {
			e.confirmation <- &confirmation{0, err}
			return
		}
	}

	lastLogFile := self.logFiles[len(self.logFiles)-1]
	self.state.LargestRequestNumber++
	requestNumber := self.state.LargestRequestNumber
	err := lastLogFile.appendRequest(e.request, e.shardId)
	if err != nil {
		e.confirmation <- &confirmation{0, err}
		return
	}
	// TODO: conditionaly index and write the state
	e.confirmation <- &confirmation{requestNumber, nil}
}

func (self *WAL) conditionalBookmarkAndIndex() {
	shouldFlush := false
	self.state.RequestsSinceLastIndex++
	if self.state.RequestsSinceLastIndex >= self.config.WalIndexAfterRequests {
		shouldFlush = true
		self.forceIndex()
	}

	self.state.RequestsSinceLastBookmark++
	if self.state.RequestsSinceLastBookmark >= self.config.WalBookmarkAfterRequests {
		shouldFlush = true
		self.forceBookmark()
	}

	self.state.RequestsSinceLastFlush++
	if self.state.RequestsSinceLastFlush > self.config.WalFlushAfterRequests || shouldFlush {
		self.forceFlush()
	}
}

func (self *WAL) forceFlush() error {
	// TODO: implement
	return nil
}

func (self *WAL) forceBookmark() error {
	bookmarkPath := path.Join(self.config.WalDir, "bookmark.new")
	bookmarkFile, err := os.OpenFile(bookmarkPath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer bookmarkFile.Close()
	self.state.FileOffset = self.LastLogFile().fileSize
	if err := self.state.write(bookmarkFile); err != nil {
		return err
	}
	if err := bookmarkFile.Close(); err != nil {
		return err
	}
	err = os.Rename(bookmarkPath, path.Join(self.config.WalDir, "bookmark"))
	if err != nil {
		return err
	}
	self.state.RequestsSinceLastBookmark = 0
	return nil
}

func (self *WAL) LastLogFile() *log {
	return self.logFiles[len(self.logFiles)-1]
}

func (self *WAL) forceIndex() error {
	// don't do anything if the number of requests writtern since the
	// last index update is 0
	if self.state.RequestsSinceLastIndex == 0 {
		return nil
	}

	startRequestNumber := self.state.LargestRequestNumber - uint32(self.state.RequestsSinceLastIndex) + 1
	logger.Debug("Creating new index entry [%d,%d]", startRequestNumber, self.state.RequestsSinceLastIndex)
	self.state.LastLogFile().Index.addEntry(startRequestNumber, uint32(self.state.RequestsSinceLastIndex), self.LastLogFile().fileSize)
	self.state.RequestsSinceLastIndex = 0
	return nil
}

func (self *WAL) processCommitEntry(e *commitEntry) {
	self.state.commitRequestNumber(e.serverId, e.requestNumber)
	lowestCommitedRequestNumber := self.state.LowestCommitedRequestNumber()

	index := self.firstLogFile(lowestCommitedRequestNumber)
	if index == 0 {
		e.confirmation <- &confirmation{0, nil}
		return
	}

	var unusedLogFiles []*log
	unusedLogFiles, self.logFiles = self.logFiles[:index], self.logFiles[index:]
	for _, logFile := range unusedLogFiles {
		logFile.close()
		logFile.delete()
	}
	self.state.Files = self.state.Files[index:]
	e.confirmation <- &confirmation{0, nil}
}

// creates a new log file using the next suffix and initializes its
// state with the state of the last log file
func (self *WAL) createNewLog() (*log, error) {
	self.nextLogFileSuffix++
	logFileName := path.Join(self.config.WalDir, fmt.Sprintf("log.%d", self.nextLogFileSuffix))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	log, err := newLog(logFile, self.config)
	if err != nil {
		return nil, err
	}

	self.logFiles = append(self.logFiles, log)
	self.state.Files = append(self.state.Files, &logFileState{})
	return log, nil
}

// Will assign sequence numbers if null. Returns a unique id that
// should be marked as committed for each server as it gets confirmed.
func (self *WAL) AssignSequenceNumbersAndLog(request *protocol.Request, shard Shard) (uint32, error) {
	confirmationChan := make(chan *confirmation)
	self.entries <- &appendEntry{confirmationChan, request, shard.Id()}
	confirmation := <-confirmationChan
	return confirmation.requestNumber, confirmation.err
}

func (self *WAL) doesLogFileContainRequest(order RequestNumberOrder, requestNumber uint32) func(int) bool {
	return func(i int) bool {
		if order.isAfter(self.state.Files[i].FirstRequestNumber, requestNumber) {
			return true
		}
		return false
	}
}

// returns the first log file that contains the given request number
func (self *WAL) firstLogFile(requestNumber uint32) int {
	lengthLogFiles := len(self.logFiles)

	if self.state.isAfterOrEqual(requestNumber, self.state.LastLogFile().FirstRequestNumber) {
		return lengthLogFiles - 1
	} else if self.state.isAfterOrEqual(self.state.FirstLogFile().FirstRequestNumber, requestNumber) {
		return 0
	}
	return sort.Search(lengthLogFiles, self.doesLogFileContainRequest(self.state, requestNumber)) - 1
}

func (self *WAL) shouldRotateTheLogFile() bool {
	lastFile := self.state.LastLogFile()
	numberOfRequests := lastFile.LastRequestNumber - lastFile.FirstRequestNumber + 1
	return numberOfRequests >= uint32(self.requestsPerLogFile)
}

func (self *WAL) rotateTheLogFile() error {
	if !self.shouldRotateTheLogFile() {
		return nil
	}

	lastLogFile, err := self.createNewLog()
	if err != nil {
		return err
	}
	logger.Info("Rotating log. New log file %s", lastLogFile.file.Name())
	return nil
}
