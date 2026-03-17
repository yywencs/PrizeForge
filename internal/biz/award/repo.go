package award

import "context"

type Repo interface {
	SaveUserAwardRecord(ctx context.Context, aggregate *UserAwardTaskInfo) error
}
