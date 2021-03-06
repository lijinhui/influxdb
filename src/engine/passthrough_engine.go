package engine

// This engine buffers points and passes them through without modification. Works for queries
// that can't be aggregated locally or queries that don't require it like deletes and drops.
import (
	"protocol"
)

type PassthroughEngine struct {
	responseChan        chan *protocol.Response
	response            *protocol.Response
	maxPointsInResponse int
	limiter             *Limiter
}

func NewPassthroughEngine(responseChan chan *protocol.Response, maxPointsInResponse int) *PassthroughEngine {
	return NewPassthroughEngineWithLimit(responseChan, maxPointsInResponse, 0)
}

func NewPassthroughEngineWithLimit(responseChan chan *protocol.Response, maxPointsInResponse, limit int) *PassthroughEngine {
	return &PassthroughEngine{
		responseChan:        responseChan,
		maxPointsInResponse: maxPointsInResponse,
		limiter:             NewLimiter(limit),
	}
}

func (self *PassthroughEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	series := &protocol.Series{Name: seriesName, Points: []*protocol.Point{point}, Fields: columnNames}
	self.limiter.calculateLimitAndSlicePoints(series)
	if len(series.Points) == 0 {
		return false
	}

	if self.response == nil {
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: series,
		}
	} else if self.response.Series.Name != seriesName {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: series,
		}
	} else if len(self.response.Series.Points) > self.maxPointsInResponse {
		self.responseChan <- self.response
		self.response = &protocol.Response{
			Type:   &queryResponse,
			Series: series,
		}
	} else {
		self.response.Series.Points = append(self.response.Series.Points, point)
	}
	return !self.limiter.hitLimit(*seriesName)
}

func (self *PassthroughEngine) Close() {
	if self.response != nil && self.response.Series != nil && self.response.Series.Name != nil {
		self.responseChan <- self.response
	}
	response := &protocol.Response{Type: &endStreamResponse}
	self.responseChan <- response
}
