package wal

import (
	"code.google.com/p/goprotobuf/proto"
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
	state              *GlobalState
	config             *configuration.Configuration
	logFiles           []*log
	serverId           uint32
	nextLogFileSuffix  int
	requestsPerLogFile int
	entries            chan interface{}
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

	// TODO: recover state from file
	state := &GlobalState{}
	// sort the logfiles by the first request number in the log
	wal := &WAL{
		config:             config,
		logFiles:           logFiles,
		requestsPerLogFile: config.WalRequestsPerLogFile,
		nextLogFileSuffix:  nextLogFileSuffix,
		state:              state,
		entries:            make(chan interface{}, 10),
	}

	// if we don't have any log files open yet, open a new one
	if len(logFiles) == 0 {
		_, err = wal.createNewLog()
	} else {
		sort.Sort(sortableLogSlice{logFiles, state})
	}

	go wal.processEntries()

	return wal, err
}

func (self *WAL) SetServerId(id uint32) {
	self.serverId = id
	for _, log := range self.logFiles {
		log.setServerId(id)
	}
}

// Marks a given request for a given server as committed
func (self *WAL) Commit(requestNumber uint32, serverId uint32) error {
	confirmationChan := make(chan *confirmation)
	self.entries <- &commitEntry{confirmationChan, serverId, requestNumber}
	confirmation := <-confirmationChan
	return confirmation.err
}

func (self *WAL) RecoverServerFromLastCommit(serverId uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	lastLogFile := self.logFiles[len(self.logFiles)-1]
	requestNumber := self.state.ServerLastRequestNumber[serverId]
	return self.RecoverServerFromRequestNumber(requestNumber, shardIds, yield)
}

// In the case where this server is running and another one in the cluster stops responding, at some point this server will have to just write
// requests to disk. When the downed server comes back up, it's this server's responsibility to send out any writes that were queued up. If
// the yield function returns nil then the request is committed.
func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	var firstLogFile int

outer:
	for _, logFile := range self.logFiles[firstLogFile:] {
		logger.Info("Replaying from %s", logFile.file.Name())
		count := 0
		ch, stopChan := logFile.replayFromRequestNumber(shardIds, requestNumber, self.state)
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

func (self *WAL) assignSequenceNumbersAndRequestNumber(shardId uint32, request *protocol.Request) {
	if request.Series == nil {
		return
	}
	sequenceNumber := self.state.getCurrentSequenceNumber(shardId)
	for _, p := range request.Series.Points {
		if p.SequenceNumber != nil {
			continue
		}
		sequenceNumber++
		p.SequenceNumber = proto.Uint64(sequenceNumber*HOST_ID_OFFSET + self.serverId)
	}
	self.state.setCurrentSequenceNumber(shardId, sequenceNumber)
}

func (self *WAL) processAppendEntry(e *appendEntry) {
	if self.shouldRotateTheLogFile() {
		if err := self.rotateTheLogFile(); err != nil {
			e.confirmation <- &confirmation{0, err}
			return
		}
	}

	lastLogFile := self.logFiles[len(self.logFiles)-1]
	self.assignSequenceNumbersAndRequestNumber(e.shardId, e.request)

	requestNumber, err := lastLogFile.appendRequest(e.request, e.shardId)
	if err != nil {
		e.confirmation <- &confirmation{0, err}
		return
	}
	e.confirmation <- &confirmation{requestNumber, nil}
}

func (self *WAL) processCommitEntry(e *commitEntry) {
	lastLogFile := self.logFiles[len(self.logFiles)-1]
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
	self.state.FirstRequestNumber = self.logFiles[0].firstRequestNumber()
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
	log.setServerId(self.serverId)

	// TODO: this is ugly, we have to copy some of the state to the new
	// log. Find a better way to do this, possibly separating the state
	// that we need to keep between log files in their own file
	if len(self.logFiles) > 0 {
		lastLogFile := self.logFiles[len(self.logFiles)-1]
		// update the new state to continue from where the last log file
		// left off
	}
	self.logFiles = append(self.logFiles, log)
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
		if order.isAfter(self.logFiles[i].firstRequestNumber(), requestNumber) {
			return true
		}
		return false
	}
}

// returns the first log file that contains the given request number
func (self *WAL) firstLogFile(requestNumber uint32) int {
	lengthLogFiles := len(self.logFiles)

	lastLogFile := self.logFiles[len(self.logFiles)-1]
	state := self.state

	if state.isAfterOrEqual(requestNumber, lastLogFile.firstRequestNumber()) {
		return lengthLogFiles - 1
	} else if state.isAfterOrEqual(self.logFiles[0].firstRequestNumber(), requestNumber) {
		return 0
	}
	return sort.Search(lengthLogFiles, self.doesLogFileContainRequest(state, requestNumber)) - 1
}

func (self *WAL) shouldRotateTheLogFile() bool {
	lastLogFile := self.logFiles[len(self.logFiles)-1]
	return lastLogFile.state.TotalNumberOfRequests >= self.requestsPerLogFile
}

func (self *WAL) rotateTheLogFile() error {
	if !self.shouldRotateTheLogFile() {
		return nil
	}

	lastLogFile := self.logFiles[len(self.logFiles)-1]
	self.state.FirstRequestNumber = self.logFiles[0].firstRequestNumber()
	err := lastLogFile.forceBookmark()
	if err != nil {
		return err
	}
	lastLogFile, err = self.createNewLog()
	if err != nil {
		return err
	}
	logger.Info("Rotating log. New log file %s", lastLogFile.file.Name())
	return nil
}

// TODO: fix the following methods

func (self *WAL) conditionalBookmarkAndIndex() {
	lastLogFile := self.logFiles[len(self.logFiles)-1]

	lastLogFile.state.TotalNumberOfRequests++

	shouldFlush := false
	self.state.RequestsSinceLastIndex++
	if self.state.RequestsSinceLastIndex >= uint32(self.config.WalIndexAfterRequests) {
		shouldFlush = true
		self.forceIndex()
	}

	self.state.RequestsSinceLastBookmark++
	if self.state.RequestsSinceLastBookmark >= self.config.WalBookmarkAfterRequests {
		shouldFlush = true
		self.forceBookmark()
	}

	self.requestsSinceLastFlush++
	if self.requestsSinceLastFlush > self.config.WalFlushAfterRequests || shouldFlush {
		self.internalFlush()
	}
}

func (self *log) forceBookmark() error {
	logger.Debug("Creating bookmark at file offset %d", self.fileSize)
	dir := filepath.Dir(self.file.Name())
	bookmarkPath := filepath.Join(dir, fmt.Sprintf("bookmark.%d.new", self.suffix()))
	bookmarkFile, err := os.OpenFile(bookmarkPath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer bookmarkFile.Close()
	self.state.setFileOffset(int64(self.fileSize))
	if err := self.state.write(bookmarkFile); err != nil {
		return err
	}
	if err := bookmarkFile.Close(); err != nil {
		return err
	}
	err = os.Rename(bookmarkPath, filepath.Join(dir, fmt.Sprintf("bookmark.%d", self.suffix())))
	if err != nil {
		return err
	}
	self.state.RequestsSinceLastBookmark = 0
	return nil
}

func (self *log) forceIndex() error {
	// don't do anything if the number of requests writtern since the
	// last index update is 0
	if self.state.RequestsSinceLastIndex == 0 {
		return nil
	}

	startRequestNumber := self.state.LargestRequestNumber - uint32(self.state.RequestsSinceLastIndex) + 1
	logger.Debug("Creating new index entry [%d,%d]", startRequestNumber, self.state.RequestsSinceLastIndex)
	self.state.Index.addEntry(startRequestNumber, self.state.RequestsSinceLastIndex, self.fileSize)
	self.state.RequestsSinceLastIndex = 0
	return nil
}

func (self *log) delete() {
	filePath := path.Join(self.config.WalDir, fmt.Sprintf("bookmark.%d", self.suffix()))
	os.Remove(filePath)
	filePath = path.Join(self.config.WalDir, fmt.Sprintf("log.%d", self.suffix()))
	os.Remove(filePath)
}
