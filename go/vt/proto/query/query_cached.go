package query

func assertCachedPb(pb []byte, tag int) {

}

func NewCachedExecuteResponse(pb []byte) *ExecuteResponse {
	assertCachedPb(pb, 1)
	return &ExecuteResponse{unknownFields: pb}
}

func (q *ExecuteResponse) CachedVT() []byte {
	return q.unknownFields
}

func CachedBeginExecuteResponse(pb []byte) *BeginExecuteResponse {
	assertCachedPb(pb, 2)
	return &BeginExecuteResponse{unknownFields: pb}
}

func (q *BeginExecuteResponse) CachedVT() []byte {
	if q.Result == nil && q.SessionStateChanges == "" && q.TabletAlias == nil && q.Error == nil && q.TransactionId == 0 {
		return q.unknownFields
	}
	return nil
}

func CachedReserveExecuteResponse(pb []byte) *ReserveExecuteResponse {
	assertCachedPb(pb, 2)
	return &ReserveExecuteResponse{unknownFields: pb}
}

func (q *ReserveExecuteResponse) CachedVT() []byte {
	if q.Error == nil && q.Result == nil && q.ReservedId == 0 && q.TabletAlias == nil {
		return q.unknownFields
	}
	return nil
}

func CachedReserveBeginExecuteResponse(pb []byte) *ReserveBeginExecuteResponse {
	assertCachedPb(pb, 2)
	return &ReserveBeginExecuteResponse{unknownFields: pb}
}

func (q *ReserveBeginExecuteResponse) CachedVT() []byte {
	if q.Error == nil && q.Result == nil && q.TransactionId == 0 && q.ReservedId == 0 && q.TabletAlias == nil && q.SessionStateChanges == "" {
		return q.unknownFields
	}
	return nil
}

func CachedStreamExecuteResponse(pb []byte) *StreamExecuteResponse {
	assertCachedPb(pb, 1)
	return &StreamExecuteResponse{unknownFields: pb}
}

func (q *StreamExecuteResponse) CachedVT() []byte {
	return q.unknownFields
}

func CachedBeginStreamExecuteResponse(pb []byte) *BeginStreamExecuteResponse {
	assertCachedPb(pb, 2)
	return &BeginStreamExecuteResponse{unknownFields: pb}
}

func (q *BeginStreamExecuteResponse) CachedVT() []byte {
	if q.Error == nil && q.Result == nil && q.TransactionId == 0 && q.TabletAlias == nil && q.SessionStateChanges == "" {
		return q.unknownFields
	}
	return nil
}

func CachedReserveStreamExecuteResponse(pb []byte) *ReserveStreamExecuteResponse {
	assertCachedPb(pb, 2)
	return &ReserveStreamExecuteResponse{unknownFields: pb}
}

func (q *ReserveStreamExecuteResponse) CachedVT() []byte {
	if q.Error == nil && q.Result == nil && q.TabletAlias == nil && q.ReservedId == 0 {
		return q.unknownFields
	}
	return nil
}

func CachedReserveBeginStreamExecuteResponse(pb []byte) *ReserveBeginStreamExecuteResponse {
	assertCachedPb(pb, 2)
	return &ReserveBeginStreamExecuteResponse{unknownFields: pb}
}

func (q *ReserveBeginStreamExecuteResponse) CachedVT() []byte {
	if q.Error == nil && q.Result == nil && q.TransactionId == 0 && q.ReservedId == 0 && q.TabletAlias == nil && q.SessionStateChanges == "" {
		return q.unknownFields
	}
	return nil
}
