package analytics

import "context"

// Namespace inspection must work before the isolated database exists.
func (c *ClickHouse) QueryGlobal(ctx context.Context, query string) ([]byte, error) {
	return c.request(ctx, query, nil, nil, false)
}
