package listener

import (
	"big-market-kratos/pkg/logger"
	"context"
)

type AwardSendListener struct {
}

func NewAwardSendListener() *AwardSendListener {
	return &AwardSendListener{}
}

func (l *AwardSendListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	logger.Info("AwardSendListener received message", "body", string(body))
	return false, nil
}
