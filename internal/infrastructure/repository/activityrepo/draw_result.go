package activityrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"time"
)

const (
	drawProcessingLease     = 30 * time.Second
	drawRequestResultTTL    = 7 * 24 * time.Hour
	drawResultRecoveryBatch = 100
)

const claimDrawScript = `
	local pending = redis.call('GET', KEYS[1])
	if not pending then
		return {-1, ''}
	end
	local ok, reservation = pcall(cjson.decode, pending)
	if not ok or reservation.order_id ~= ARGV[1] then
		return {-2, ''}
	end

	if reservation.draw_state == 'success' then
		local result = redis.call('GET', KEYS[2])
		return {2, result or ''}
	end
	if reservation.draw_state == 'cancelled' then
		return {3, ''}
	end

	local now = tonumber(ARGV[3])
	local stale_before = tonumber(ARGV[4])
	if reservation.draw_state == 'created' or
	   (reservation.draw_state == 'processing' and
	    (not reservation.processing_at or tonumber(reservation.processing_at) < stale_before)) then
		reservation.draw_state = 'processing'
		reservation.processing_at = now
		reservation.draw_owner = ARGV[2]
		redis.call('SET', KEYS[1], cjson.encode(reservation))
		return {0, ARGV[2]}
	end
	return {1, ''}
`

const releaseDrawClaimScript = `
	local pending = redis.call('GET', KEYS[1])
	if not pending then
		return 0
	end
	local ok, reservation = pcall(cjson.decode, pending)
	if not ok or reservation.order_id ~= ARGV[1] then
		return -1
	end
	if reservation.draw_state == 'processing' and reservation.draw_owner == ARGV[2] then
		reservation.draw_state = 'created'
		reservation.processing_at = nil
		reservation.draw_owner = nil
		redis.call('SET', KEYS[1], cjson.encode(reservation))
		return 1
	end
	return 0
`

const completeDrawScript = `
	local pending = redis.call('GET', KEYS[1])
	if not pending then
		return {-1, ''}
	end
	local pending_ok, reservation = pcall(cjson.decode, pending)
	if not pending_ok or reservation.order_id ~= ARGV[1] or reservation.request_id ~= ARGV[2] then
		return {-2, ''}
	end

	local existing = redis.call('GET', KEYS[2])
	if existing then
		return {1, existing}
	end
	if reservation.draw_state ~= 'processing' or reservation.draw_owner ~= ARGV[3] then
		return {-3, ''}
	end

	local result_ok, result = pcall(cjson.decode, ARGV[4])
	if not result_ok or result.order_id ~= ARGV[1] or result.request_id ~= ARGV[2] then
		return {-4, ''}
	end

	local stream_id = redis.call('XADD', KEYS[3], '*', 'event', ARGV[4])
	local publication = {
		stream_id = stream_id,
		broker_confirmed = false,
		result = result
	}
	local stored_publication = cjson.encode(publication)

	reservation.draw_state = 'success'
	reservation.processing_at = nil
	reservation.draw_owner = nil
	redis.call('SET', KEYS[1], cjson.encode(reservation))
	redis.call('SET', KEYS[2], stored_publication, 'EX', ARGV[5])
	return {0, stored_publication}
`

const markDrawResultPublishedScript = `
	local raw = redis.call('GET', KEYS[1])
	if not raw then
		return -1
	end
	local ok, publication = pcall(cjson.decode, raw)
	if not ok or not publication.result or
	   publication.result.order_id ~= ARGV[1] or publication.stream_id ~= ARGV[2] then
		return -2
	end
	publication.broker_confirmed = true
	redis.call('SET', KEYS[1], cjson.encode(publication), 'KEEPTTL')
	redis.call('XDEL', KEYS[2], ARGV[2])
	return 1
`

const queryDrawResultStreamScript = `
	return redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', ARGV[1])
`

const clearPersistedPendingScript = `
	local pending = redis.call('GET', KEYS[1])
	if not pending then
		return 0
	end
	local ok, reservation = pcall(cjson.decode, pending)
	if not ok or reservation.order_id ~= ARGV[1] then
		return -1
	end
	redis.call('DEL', KEYS[1])
	return 1
`

func (r *Repository) TryClaimUserRaffleOrder(ctx context.Context, userID string, activityID int64, requestID string, orderID string) (*activity.DrawClaim, error) {
	owner, err := newDrawOwner()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	result, err := r.redis.Eval(
		ctx,
		claimDrawScript,
		[]string{
			adapter.GetPendingRaffleOrderKey(activityID, userID),
			adapter.GetDrawRequestResultKey(activityID, userID, requestID),
		},
		orderID,
		owner,
		now.UnixMilli(),
		now.Add(-drawProcessingLease).UnixMilli(),
	)
	if err != nil {
		return nil, err
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return nil, fmt.Errorf("unexpected draw claim result %#v", result)
	}
	status, ok := values[0].(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected draw claim status %#v", values[0])
	}
	switch status {
	case 0:
		return &activity.DrawClaim{Status: activity.DrawClaimAcquired, Owner: owner}, nil
	case 1:
		return &activity.DrawClaim{Status: activity.DrawClaimProcessing}, nil
	case 2:
		payload, _ := values[1].(string)
		if payload == "" {
			return nil, activity.ErrRecordNotFound
		}
		var publication activity.DrawResultPublication
		if err := json.Unmarshal([]byte(payload), &publication); err != nil {
			return nil, fmt.Errorf("decode completed draw claim publication: %w", err)
		}
		if publication.Result == nil || publication.StreamID == "" {
			return nil, errors.New("completed draw claim publication is invalid")
		}
		return &activity.DrawClaim{Status: activity.DrawClaimCompleted, Publication: &publication}, nil
	case 3:
		return &activity.DrawClaim{Status: activity.DrawClaimCancelled}, nil
	case -1:
		return nil, activity.ErrRecordNotFound
	default:
		return nil, fmt.Errorf("invalid draw reservation while claiming order %s: status=%d", orderID, status)
	}
}

func (r *Repository) ReleaseUserRaffleOrderClaim(ctx context.Context, userID string, activityID int64, orderID string, owner string) error {
	result, err := r.redis.Eval(
		ctx,
		releaseDrawClaimScript,
		[]string{adapter.GetPendingRaffleOrderKey(activityID, userID)},
		orderID,
		owner,
	)
	if err != nil {
		return err
	}
	status, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected release draw claim result %#v", result)
	}
	if status < 0 {
		return errors.New("draw reservation identity mismatch")
	}
	return nil
}

func (r *Repository) CompleteUserRaffleOrder(ctx context.Context, result *activity.DrawResult, owner string) (*activity.DrawResultPublication, error) {
	if result == nil || result.UserID == "" || result.ActivityID <= 0 || result.RequestID == "" ||
		result.OrderID == "" || result.OrderTime.IsZero() || result.AwardID <= 0 ||
		result.AwardTime.IsZero() || owner == "" {
		return nil, activity.ErrInvalidParams
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode draw result: %w", err)
	}
	raw, err := r.redis.Eval(
		ctx,
		completeDrawScript,
		[]string{
			adapter.GetPendingRaffleOrderKey(result.ActivityID, result.UserID),
			adapter.GetDrawRequestResultKey(result.ActivityID, result.UserID, result.RequestID),
			adapter.GetDrawResultStreamKey(),
		},
		result.OrderID,
		result.RequestID,
		owner,
		string(payload),
		int64(drawRequestResultTTL/time.Second),
	)
	if err != nil {
		return nil, err
	}
	values, ok := raw.([]interface{})
	if !ok || len(values) != 2 {
		return nil, fmt.Errorf("unexpected complete draw result %#v", raw)
	}
	status, ok := values[0].(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected complete draw status %#v", values[0])
	}
	if status < 0 {
		return nil, fmt.Errorf("complete draw rejected: status=%d order_id=%s", status, result.OrderID)
	}
	storedJSON, ok := values[1].(string)
	if !ok || storedJSON == "" {
		return nil, errors.New("completed draw result is empty")
	}
	var publication activity.DrawResultPublication
	if err := json.Unmarshal([]byte(storedJSON), &publication); err != nil {
		return nil, fmt.Errorf("decode stored draw publication: %w", err)
	}
	if publication.Result == nil || publication.StreamID == "" {
		return nil, errors.New("stored draw publication is invalid")
	}
	return &publication, nil
}

func (r *Repository) MarkDrawResultPublished(ctx context.Context, publication *activity.DrawResultPublication) error {
	if publication == nil || publication.Result == nil || publication.StreamID == "" {
		return activity.ErrInvalidParams
	}
	result := publication.Result
	raw, err := r.redis.Eval(
		ctx,
		markDrawResultPublishedScript,
		[]string{
			adapter.GetDrawRequestResultKey(result.ActivityID, result.UserID, result.RequestID),
			adapter.GetDrawResultStreamKey(),
		},
		result.OrderID,
		publication.StreamID,
	)
	if err != nil {
		return err
	}
	status, ok := raw.(int64)
	if !ok || status < 0 {
		return fmt.Errorf("mark draw result published failed: status=%v", raw)
	}
	publication.BrokerConfirmed = true
	return nil
}

func (r *Repository) QueryPendingDrawResultPublications(ctx context.Context, limit int64) ([]*activity.DrawResultPublication, error) {
	if limit <= 0 || limit > drawResultRecoveryBatch {
		limit = drawResultRecoveryBatch
	}
	raw, err := r.redis.Eval(
		ctx,
		queryDrawResultStreamScript,
		[]string{adapter.GetDrawResultStreamKey()},
		limit,
	)
	if err != nil {
		return nil, err
	}
	entries, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected draw result stream response %#v", raw)
	}
	publications := make([]*activity.DrawResultPublication, 0, len(entries))
	for _, entryRaw := range entries {
		entry, ok := entryRaw.([]interface{})
		if !ok || len(entry) != 2 {
			return nil, fmt.Errorf("invalid draw result stream entry %#v", entryRaw)
		}
		streamID, ok := entry[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid draw result stream id %#v", entry[0])
		}
		fields, ok := entry[1].([]interface{})
		if !ok || len(fields) != 2 || fields[0] != "event" {
			return nil, fmt.Errorf("invalid draw result stream fields %#v", entry[1])
		}
		eventJSON, ok := fields[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid draw result stream event %#v", fields[1])
		}
		var result activity.DrawResult
		if err := json.Unmarshal([]byte(eventJSON), &result); err != nil {
			return nil, fmt.Errorf("decode draw result stream event: %w", err)
		}
		publications = append(publications, &activity.DrawResultPublication{
			StreamID:        streamID,
			BrokerConfirmed: false,
			Result:          &result,
		})
	}
	return publications, nil
}

func (r *Repository) clearPersistedPendingDraw(ctx context.Context, result *activity.DrawResult) error {
	raw, err := r.redis.Eval(
		ctx,
		clearPersistedPendingScript,
		[]string{adapter.GetPendingRaffleOrderKey(result.ActivityID, result.UserID)},
		result.OrderID,
	)
	if err != nil {
		return err
	}
	status, ok := raw.(int64)
	if !ok || status < 0 {
		return fmt.Errorf("clear persisted pending draw failed: status=%v", raw)
	}
	return nil
}
