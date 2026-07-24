//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"prizeforge/internal/domain/activity"
	"prizeforge/internal/domain/award"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/activityrepo"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/internal/listener"
	"prizeforge/pkg/rabbitmq"
	"prizeforge/pkg/xrand"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMQConsumerDispatchesStockZeroEventAndAcknowledges 验证真实 RabbitMQ 消息会被
// Consumer 分发给库存 Listener，Listener 将 MySQL SKU 库存清零，并在成功后 ACK 消息。
func TestRabbitMQConsumerDispatchesStockZeroEventAndAcknowledges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := "prizeforge.integration.consumer.stock-zero." + xrand.RandomNumeric(12)
	trackIntegrationRabbitMQTopology(t, topic)
	connection, err := adapter.NewConnection(integrationRabbitMQConfig)
	if err != nil {
		t.Fatalf("connect RabbitMQ consumer test: %v", err)
	}
	consumer := listener.NewRabbitMQConsumer(connection)
	t.Cleanup(consumer.Shutdown)

	skuID := newIntegrationRedisActivityID(t)
	activityID := newIntegrationRedisActivityID(t)
	now := time.Now().Truncate(time.Second)
	fixture := &po.RaffleActivitySku{
		Sku:               skuID,
		ActivityID:        activityID,
		ActivityCountID:   newIntegrationRedisActivityID(t),
		StockCount:        5,
		StockCountSurplus: 5,
		CreateTime:        now,
		UpdateTime:        now,
	}
	if err := integrationDefaultDB.Create(fixture).Error; err != nil {
		t.Fatalf("prepare RabbitMQ consumer SKU fixture: %v", err)
	}
	t.Cleanup(func() {
		deleteIntegrationRows(t, integrationDefaultDB, "raffle_activity_sku", "sku", skuID)
	})

	repository := activityrepo.NewRepository(integrationDBRouter, integrationDefaultDB, integrationRedis, nil, nil, nil)
	quotaUsecase := activity.NewActivityQuotaUsecase(repository)
	stockListener := listener.NewActivityStockListener(quotaUsecase)
	recordingListener := newIntegrationRecordingListener(stockListener, 2)
	consumer.RegisterListener(topic, recordingListener)
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start RabbitMQ consumer: %v", err)
	}

	rabbitPublisher, err := adapter.NewRabbitMQPublisher(connection, 1)
	if err != nil {
		t.Fatalf("create stock-zero publisher: %v", err)
	}
	publisherConfig := *integrationRabbitMQConfig
	publisherConfig.Topic.ActivitySkuStockZero = topic
	publisher := adapter.NewPublisher(rabbitPublisher, &publisherConfig)
	firstEvent := rabbitmq.NewBaseEvent(skuID)
	secondEvent := rabbitmq.NewBaseEvent(skuID)
	if err := publisher.PublishStockZero(ctx, firstEvent); err != nil {
		t.Fatalf("publish first stock-zero event: %v", err)
	}
	if err := publisher.PublishStockZero(ctx, secondEvent); err != nil {
		t.Fatalf("publish second stock-zero event: %v", err)
	}

	firstCall := waitIntegrationListenerCall(t, ctx, recordingListener.calls)
	secondCall := waitIntegrationListenerCall(t, ctx, recordingListener.calls)
	assertIntegrationSuccessfulListenerCall(t, firstCall, firstEvent.ID, skuID)
	assertIntegrationSuccessfulListenerCall(t, secondCall, secondEvent.ID, skuID)

	// Consumer 设置了 prefetch=1；第二条消息能够进入 Listener，说明第一条已经 ACK。
	var stored po.RaffleActivitySku
	if err := integrationDefaultDB.Where("sku = ?", skuID).First(&stored).Error; err != nil {
		t.Fatalf("query SKU after stock-zero consumption: %v", err)
	}
	if stored.StockCountSurplus != 0 {
		t.Fatalf("SKU stock surplus = %d, want 0", stored.StockCountSurplus)
	}
}

// TestRabbitMQConsumerProcessesOneQueueWithMultipleIndependentChannels 验证同一队列
// 配置三个消费者后，prefetch=1 时仍能同时进入三个阻塞中的 Listener。
func TestRabbitMQConsumerProcessesOneQueueWithMultipleIndependentChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := "prizeforge.integration.consumer.concurrent." + xrand.RandomNumeric(12)
	trackIntegrationRabbitMQTopology(t, topic)
	connection, err := adapter.NewConnection(integrationRabbitMQConfig)
	if err != nil {
		t.Fatalf("connect RabbitMQ concurrent consumer test: %v", err)
	}
	consumer := listener.NewRabbitMQConsumer(
		connection,
		listener.WithPrefetch(1),
		listener.WithQueueConcurrency(map[string]int{topic + "_queue": 3}),
	)
	t.Cleanup(consumer.Shutdown)

	blockingListener := newIntegrationBlockingListener(3)
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(blockingListener.release) })
	})
	consumer.RegisterListener(topic, blockingListener)
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start concurrent RabbitMQ consumer: %v", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open queue inspection channel: %v", err)
	}
	queue, err := channel.QueueInspect(topic + "_queue")
	_ = channel.Close()
	if err != nil {
		t.Fatalf("inspect concurrent consumer queue: %v", err)
	}
	if queue.Consumers != 3 {
		t.Fatalf("queue consumers = %d, want 3", queue.Consumers)
	}

	publisher := newIntegrationTopicPublisher(t, connection)
	for i := 1; i <= 3; i++ {
		if err := publisher.PublishTopic(ctx, topic, rabbitmq.NewBaseEvent(int64(i))); err != nil {
			t.Fatalf("publish concurrent event %d: %v", i, err)
		}
	}

	for i := 1; i <= 3; i++ {
		select {
		case <-blockingListener.started:
		case <-ctx.Done():
			t.Fatalf("only %d/3 listeners entered concurrently: %v", i-1, ctx.Err())
		}
	}
	releaseOnce.Do(func() { close(blockingListener.release) })
	for i := 1; i <= 3; i++ {
		select {
		case err := <-blockingListener.completed:
			if err != nil {
				t.Fatalf("concurrent listener %d error = %v", i, err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for concurrent listener %d: %v", i, ctx.Err())
		}
	}
}

// TestRabbitMQConsumerPersistsDrawResultAndAcknowledges 验证完整抽奖结果经过真实 RabbitMQ 后，
// 会在一个事务内落订单、中奖记录、发奖 Outbox，并同步扣减数据库总、月、日额度。
func TestRabbitMQConsumerPersistsDrawResultAndAcknowledges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fixture := newActivityOrderFixture(t, 3, 3, 3)
	repository := fixture.repository()
	order := fixture.order(xrand.RandomNumeric(12), "request-"+xrand.RandomNumeric(12))
	result := fixture.drawResult(order)

	topic := "prizeforge.integration.consumer.draw-result." + xrand.RandomNumeric(12)
	trackIntegrationRabbitMQTopology(t, topic)
	connection, err := adapter.NewConnection(integrationRabbitMQConfig)
	if err != nil {
		t.Fatalf("connect RabbitMQ draw-result test: %v", err)
	}
	consumer := listener.NewRabbitMQConsumer(connection)
	t.Cleanup(consumer.Shutdown)
	partakeUsecase := activity.NewActivityPartakeUsecase(repository)
	recordingListener := newIntegrationRecordingListener(listener.NewDrawResultListener(partakeUsecase), 1)
	consumer.RegisterListener(topic, recordingListener)
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start draw-result consumer: %v", err)
	}

	rabbitPublisher, err := adapter.NewRabbitMQPublisher(connection, 1)
	if err != nil {
		t.Fatalf("create draw-result publisher: %v", err)
	}
	publisherConfig := *integrationRabbitMQConfig
	publisherConfig.Topic.DrawResult = topic
	publisher := adapter.NewPublisher(rabbitPublisher, &publisherConfig)
	event := &rabbitmq.BaseEvent{
		ID:        "draw:" + result.UserID + ":" + result.OrderID,
		Timestamp: result.AwardTime,
		Data:      result,
	}
	if err := publisher.PublishDrawResult(ctx, event); err != nil {
		t.Fatalf("publish draw-result event: %v", err)
	}
	call := waitIntegrationListenerCall(t, ctx, recordingListener.calls)
	if call.retry || call.err != nil {
		t.Fatalf("draw-result listener = retry:%t err:%v, want success", call.retry, call.err)
	}

	assertActivityOrderCount(t, fixture, 1)
	assertActivityAccountState(t, fixture, 2, 3, 3, "")
	assertActivityPeriodQuota(t, fixture, 2, 2)
	var awardCount, outboxCount int64
	if err := fixture.db.Table(fixture.awardTable).
		Where("user_id = ? AND order_id = ?", fixture.userID, order.OrderID).
		Count(&awardCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Table("task").
		Where("user_id = ?", fixture.userID).
		Count(&outboxCount).Error; err != nil {
		t.Fatal(err)
	}
	if awardCount != 1 || outboxCount != 2 {
		t.Fatalf("persisted counts award=%d outbox=%d, want 1/2", awardCount, outboxCount)
	}
}

// TestRabbitMQConsumerRequeuesRetryableFailure 验证 Listener 报告临时错误时，Consumer 会
// NACK 并重新入队；同一条消息第二次处理成功后才能完成消费。
func TestRabbitMQConsumerRequeuesRetryableFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := "prizeforge.integration.consumer.retry." + xrand.RandomNumeric(12)
	trackIntegrationRabbitMQTopology(t, topic)
	connection, err := adapter.NewConnection(integrationRabbitMQConfig)
	if err != nil {
		t.Fatalf("connect RabbitMQ retry test: %v", err)
	}
	consumer := listener.NewRabbitMQConsumer(connection)
	t.Cleanup(consumer.Shutdown)
	retryListener := newIntegrationRetryOnceListener()
	consumer.RegisterListener(topic, retryListener)
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start RabbitMQ retry consumer: %v", err)
	}

	publisher := newIntegrationTopicPublisher(t, connection)
	event := rabbitmq.NewBaseEvent(int64(7_000_002))
	if err := publisher.PublishTopic(ctx, topic, event); err != nil {
		t.Fatalf("publish retryable event: %v", err)
	}

	firstCall := waitIntegrationListenerCall(t, ctx, retryListener.calls)
	secondCall := waitIntegrationListenerCall(t, ctx, retryListener.calls)
	if firstCall.attempt != 1 || !firstCall.retry || firstCall.err == nil {
		t.Fatalf("first listener call = attempt:%d retry:%t err:%v, want retryable failure", firstCall.attempt, firstCall.retry, firstCall.err)
	}
	if secondCall.attempt != 2 || secondCall.retry || secondCall.err != nil {
		t.Fatalf("second listener call = attempt:%d retry:%t err:%v, want success", secondCall.attempt, secondCall.retry, secondCall.err)
	}
	if string(firstCall.body) != string(secondCall.body) {
		t.Fatal("requeued RabbitMQ message body changed between attempts")
	}
	assertIntegrationEventBody(t, secondCall.body, event.ID, 7_000_002)
}

// TestRabbitMQConsumerCompletesAwardAndAcknowledgesIdempotently 验证 send_award 消息经过
// 真实 RabbitMQ 后，会将分片中奖记录更新为 complete；重复投递同一消息仍会成功 ACK。
func TestRabbitMQConsumerCompletesAwardAndAcknowledgesIdempotently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fixture := newAwardTransactionFixture(t)
	awardUsecase := newIntegrationAwardUsecase()
	if _, err := awardUsecase.SaveUserAwardRecord(ctx, fixture.awardRecord()); err != nil {
		t.Fatalf("prepare award record and outbox: %v", err)
	}

	topic := "prizeforge.integration.consumer.send-award." + xrand.RandomNumeric(12)
	trackIntegrationRabbitMQTopology(t, topic)
	connection, err := adapter.NewConnection(integrationRabbitMQConfig)
	if err != nil {
		t.Fatalf("connect RabbitMQ send-award test: %v", err)
	}
	consumer := listener.NewRabbitMQConsumer(connection)
	t.Cleanup(consumer.Shutdown)
	recordingListener := newIntegrationRecordingListener(listener.NewSendAwardListener(awardUsecase), 2)
	consumer.RegisterListener(topic, recordingListener)
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start send-award consumer: %v", err)
	}

	event := &rabbitmq.BaseEvent{
		ID:        fixture.userID + ":" + fixture.orderID,
		Timestamp: time.Now(),
		Data: award.SendAwardMessage{
			UserID:     fixture.userID,
			OrderID:    fixture.orderID,
			AwardID:    integrationAwardID,
			AwardTitle: "集成测试奖品",
		},
	}
	rabbitPublisher, err := adapter.NewRabbitMQPublisher(connection, 1)
	if err != nil {
		t.Fatalf("create send-award publisher: %v", err)
	}
	publisherConfig := *integrationRabbitMQConfig
	publisherConfig.Topic.SendAward = topic
	publisher := adapter.NewPublisher(rabbitPublisher, &publisherConfig)
	for attempt := 1; attempt <= 2; attempt++ {
		if err := publisher.PublishSendAward(ctx, event); err != nil {
			t.Fatalf("publish send-award event attempt %d: %v", attempt, err)
		}
		call := waitIntegrationListenerCall(t, ctx, recordingListener.calls)
		if call.retry || call.err != nil {
			t.Fatalf("send-award listener attempt %d = retry:%t err:%v, want success", attempt, call.retry, call.err)
		}
	}

	var stored po.UserAwardRecord
	if err := fixture.db.Table(fixture.awardTable).
		Where("user_id = ? AND order_id = ?", fixture.userID, fixture.orderID).
		First(&stored).Error; err != nil {
		t.Fatalf("query award after RabbitMQ consumption: %v", err)
	}
	if stored.AwardState != string(award.AwardStateComplete) {
		t.Fatalf("award state = %q, want %q", stored.AwardState, award.AwardStateComplete)
	}
}

type integrationListenerCall struct {
	body    []byte
	retry   bool
	err     error
	attempt int
}

type integrationBlockingListener struct {
	started   chan struct{}
	release   chan struct{}
	completed chan error
}

func newIntegrationBlockingListener(concurrency int) *integrationBlockingListener {
	return &integrationBlockingListener{
		started:   make(chan struct{}, concurrency),
		release:   make(chan struct{}),
		completed: make(chan error, concurrency),
	}
}

func (l *integrationBlockingListener) Handle(ctx context.Context, _ []byte) (bool, error) {
	l.started <- struct{}{}
	select {
	case <-l.release:
		l.completed <- nil
		return false, nil
	case <-ctx.Done():
		l.completed <- ctx.Err()
		return true, ctx.Err()
	}
}

type integrationRecordingListener struct {
	delegate listener.Listener
	calls    chan integrationListenerCall
}

func newIntegrationRecordingListener(delegate listener.Listener, capacity int) *integrationRecordingListener {
	return &integrationRecordingListener{
		delegate: delegate,
		calls:    make(chan integrationListenerCall, capacity),
	}
}

func (l *integrationRecordingListener) Handle(ctx context.Context, body []byte) (bool, error) {
	retry, err := l.delegate.Handle(ctx, body)
	l.calls <- integrationListenerCall{
		body:  append([]byte(nil), body...),
		retry: retry,
		err:   err,
	}
	return retry, err
}

type integrationRetryOnceListener struct {
	mu       sync.Mutex
	attempts int
	calls    chan integrationListenerCall
}

func newIntegrationRetryOnceListener() *integrationRetryOnceListener {
	return &integrationRetryOnceListener{calls: make(chan integrationListenerCall, 2)}
}

func (l *integrationRetryOnceListener) Handle(_ context.Context, body []byte) (bool, error) {
	l.mu.Lock()
	l.attempts++
	attempt := l.attempts
	l.mu.Unlock()

	call := integrationListenerCall{body: append([]byte(nil), body...), attempt: attempt}
	if attempt == 1 {
		call.retry = true
		call.err = errors.New("temporary integration failure")
	}
	l.calls <- call
	return call.retry, call.err
}

func newIntegrationTopicPublisher(t *testing.T, connection *amqp.Connection) *adapter.Publisher {
	t.Helper()
	rabbitPublisher, err := adapter.NewRabbitMQPublisher(connection, 1)
	if err != nil {
		t.Fatalf("NewRabbitMQPublisher() error = %v, want nil", err)
	}
	publisherConfig := *integrationRabbitMQConfig
	return adapter.NewPublisher(rabbitPublisher, &publisherConfig)
}

func waitIntegrationListenerCall(t *testing.T, ctx context.Context, calls <-chan integrationListenerCall) integrationListenerCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-ctx.Done():
		t.Fatalf("wait for RabbitMQ listener call: %v", ctx.Err())
		return integrationListenerCall{}
	}
}

func assertIntegrationSuccessfulListenerCall(t *testing.T, call integrationListenerCall, eventID string, skuID int64) {
	t.Helper()
	if call.retry || call.err != nil {
		t.Fatalf("listener call = retry:%t err:%v, want success", call.retry, call.err)
	}
	assertIntegrationEventBody(t, call.body, eventID, skuID)
}

func assertIntegrationEventBody(t *testing.T, body []byte, eventID string, data int64) {
	t.Helper()
	var event struct {
		ID   string `json:"id"`
		Data int64  `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatalf("unmarshal RabbitMQ listener body: %v", err)
	}
	if event.ID != eventID || event.Data != data {
		t.Fatalf("RabbitMQ listener event = %#v, want id=%q data=%d", event, eventID, data)
	}
}

func trackIntegrationRabbitMQTopology(t *testing.T, topic string) {
	t.Helper()
	t.Cleanup(func() {
		if channel, err := integrationRabbitMQConnection.Channel(); err == nil {
			_, _ = channel.QueueDelete(topic+"_queue", false, false, false)
			_ = channel.Close()
		}
		if channel, err := integrationRabbitMQConnection.Channel(); err == nil {
			_ = channel.ExchangeDelete(topic, false, false)
			_ = channel.Close()
		}
	})
}
