package listener

import (
	"context"
	"log/slog"
)

type AwardSendListener struct {
}

func NewAwardSendListener() *AwardSendListener {
	return &AwardSendListener{}
}

func (l *AwardSendListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	slog.Info("AwardSendListener received message", "body", string(body))
	return false, nil
}
