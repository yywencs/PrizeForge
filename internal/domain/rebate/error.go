package rebate

import "prizeforge/internal/shared/xerr"

var (
	ErrDBRouterNotFound = xerr.New("DB_ROUTER_NOT_FOUND", "database router not found")
)
