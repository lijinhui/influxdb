package wal

type RequestNumberOrder struct {
	firstRequestNumber uint32
	lastRequestNumber  uint32
}

func NewRequestNumberOrder(first, last uint32) *RequestNumberOrder {
	return &RequestNumberOrder{first, last}
}

func (self *RequestNumberOrder) isAfter(left, right uint32) bool {
	if left == right {
		return false
	}
	if left > self.firstRequestNumber && right > self.firstRequestNumber {
		return left > right
	}
	if left < self.lastRequestNumber && left < self.lastRequestNumber {
		return left > right
	}
	return left > self.firstRequestNumber
}

func (self *RequestNumberOrder) isAfterOrEqual(left, right uint32) bool {
	return left == right || self.isAfter(left, right)
}

func (self *RequestNumberOrder) isBefore(left, right uint32) bool {
	return !self.isAfterOrEqual(left, right)
}

func (self *RequestNumberOrder) isBeforeOrEqual(left, right uint32) bool {
	return !self.isAfter(left, right)
}
