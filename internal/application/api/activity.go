package api

import (
	"prizeforge/internal/domain/activity"
)

// ActivityUsecase API侧活动用例——薄封装，直接委托给 domain service
type ActivityUsecase struct {
	svc *activity.ActivityPartakeUsecase
}

func NewActivityUsecase(svc *activity.ActivityPartakeUsecase) *ActivityUsecase {
	return &ActivityUsecase{svc: svc}
}
