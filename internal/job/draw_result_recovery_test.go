package job

import (
	"context"
	"testing"

	"prizeforge/internal/domain/activity"
)

type fakePendingDrawResultSource struct {
	pending []*activity.DrawResultPublication
}

func (f *fakePendingDrawResultSource) QueryPendingDrawResults(context.Context, int64) ([]*activity.DrawResultPublication, error) {
	return f.pending, nil
}

type fakeDrawResultPublicationPublisher struct {
	publications []*activity.DrawResultPublication
}

func (f *fakeDrawResultPublicationPublisher) Publish(_ context.Context, publication *activity.DrawResultPublication) error {
	f.publications = append(f.publications, publication)
	return nil
}

func TestDrawResultRecoveryJobRetriesPendingStreamEntries(t *testing.T) {
	source := &fakePendingDrawResultSource{
		pending: []*activity.DrawResultPublication{
			testDrawResultPublication("3-0", "000000000003"),
			testDrawResultPublication("4-0", "000000000004"),
		},
	}
	publisher := &fakeDrawResultPublicationPublisher{}
	job := NewDrawResultRecoveryJob(source, publisher)

	if err := job.ProcessTask(context.Background(), nil); err != nil {
		t.Fatalf("ProcessTask() error = %v", err)
	}
	if len(publisher.publications) != 2 ||
		publisher.publications[0] != source.pending[0] ||
		publisher.publications[1] != source.pending[1] {
		t.Fatalf("ProcessTask() publications=%#v, want pending publications", publisher.publications)
	}
}
