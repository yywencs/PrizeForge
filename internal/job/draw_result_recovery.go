package job

import (
	"context"
	"errors"

	"prizeforge/internal/domain/activity"
	"prizeforge/pkg/logger"

	"github.com/hibiken/asynq"
)

type pendingDrawResultSource interface {
	QueryPendingDrawResults(context.Context, int64) ([]*activity.DrawResultPublication, error)
}

type drawResultPublicationPublisher interface {
	Publish(context.Context, *activity.DrawResultPublication) error
}

// DrawResultRecoveryJob 是 Asynq 补偿任务，只负责扫描待发布结果并调用发布服务重试。
type DrawResultRecoveryJob struct {
	source    pendingDrawResultSource
	publisher drawResultPublicationPublisher
}

func NewDrawResultRecoveryJob(source pendingDrawResultSource, publisher drawResultPublicationPublisher) *DrawResultRecoveryJob {
	return &DrawResultRecoveryJob{source: source, publisher: publisher}
}

func (j *DrawResultRecoveryJob) ProcessTask(ctx context.Context, _ *asynq.Task) error {
	publications, err := j.source.QueryPendingDrawResults(ctx, 100)
	if err != nil {
		return err
	}

	var firstErr error
	for _, publication := range publications {
		if err := j.publisher.Publish(ctx, publication); err != nil {
			logger.Warn("补偿发布抽奖结果失败", "streamID", publication.StreamID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return errors.New("one or more draw results failed to publish")
	}
	return nil
}
