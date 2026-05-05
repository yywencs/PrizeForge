package listener

import "github.com/google/wire"

var ProviderSet = wire.NewSet(
	NewAwardSendListener,
	NewActivityStockListener,
	NewRebateListener,
	NewSaveOrderListener,
)
