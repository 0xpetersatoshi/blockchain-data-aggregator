package loader

import (
	"context"

	"github.com/0xpetersatoshi/blockchain-data-aggregator/internal/processor"
)

type Loader interface {
	Connect(ctx context.Context) error
	Load(ctx context.Context, records processor.RecordBatcher) error
	Close(ctx context.Context) error
}
