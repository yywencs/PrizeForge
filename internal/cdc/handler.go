package cdc

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

var shardSuffixPattern = regexp.MustCompile(`_\d{3}$`)

type EventHandler struct {
	canal.DummyEventHandler

	cfg    *Config
	writer *ESWriter
}

func NewEventHandler(cfg *Config, writer *ESWriter) *EventHandler {
	return &EventHandler{
		cfg:    cfg,
		writer: writer,
	}
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	if e == nil || e.Table == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultHTTPTimeout)
	defer cancel()

	indexName := h.cfg.LogicalIndexName(e.Table.Name)

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			if err := h.writer.Upsert(ctx, indexName, h.documentID(e.Table, row), h.rowToDocument(e.Table, row)); err != nil {
				return fmt.Errorf("insert sync %s.%s: %w", e.Table.Schema, e.Table.Name, err)
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			if err := h.writer.Delete(ctx, indexName, h.documentID(e.Table, row)); err != nil {
				return fmt.Errorf("delete sync %s.%s: %w", e.Table.Schema, e.Table.Name, err)
			}
		}
	case canal.UpdateAction:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]

			oldID := h.documentID(e.Table, oldRow)
			newID := h.documentID(e.Table, newRow)
			if oldID != newID {
				if err := h.writer.Delete(ctx, indexName, oldID); err != nil {
					return fmt.Errorf("delete stale document %s.%s: %w", e.Table.Schema, e.Table.Name, err)
				}
			}

			if err := h.writer.Upsert(ctx, indexName, newID, h.rowToDocument(e.Table, newRow)); err != nil {
				return fmt.Errorf("update sync %s.%s: %w", e.Table.Schema, e.Table.Name, err)
			}
		}
	}

	return nil
}

func (h *EventHandler) OnDDL(_ *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	log.Printf("cdc ddl next_pos=%s:%d sql=%s", nextPos.Name, nextPos.Pos, strings.TrimSpace(string(queryEvent.Query)))
	return nil
}

func (h *EventHandler) OnPosSynced(_ *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, force bool) error {
	log.Printf("cdc pos synced file=%s pos=%d force=%t", pos.Name, pos.Pos, force)
	return nil
}

func (h *EventHandler) String() string {
	return "prizeforge-cdc-handler"
}

func (h *EventHandler) rowToDocument(table *schema.Table, row []any) map[string]any {
	doc := make(map[string]any, len(table.Columns)+4)
	for idx, column := range table.Columns {
		if idx >= len(row) {
			continue
		}
		doc[column.Name] = normalizeValue(row[idx])
	}

	doc["_schema"] = table.Schema
	doc["_physical_table"] = table.Name
	doc["_logical_table"] = trimShardSuffix(table.Name)
	doc["_synced_at"] = time.Now().UTC().Format(time.RFC3339)

	return doc
}

func (h *EventHandler) documentID(table *schema.Table, row []any) string {
	pkValues, err := table.GetPKValues(row)
	if err == nil && len(pkValues) > 0 {
		parts := make([]string, 0, len(pkValues))
		for _, value := range pkValues {
			parts = append(parts, fmt.Sprintf("%v", normalizeValue(value)))
		}
		return strings.Join(parts, ":")
	}

	payload, _ := json.Marshal(row)
	sum := sha1.Sum([]byte(table.Schema + "." + table.Name + ":" + string(payload)))
	return hex.EncodeToString(sum[:])
}

func normalizeValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

func trimShardSuffix(name string) string {
	return shardSuffixPattern.ReplaceAllString(strings.ToLower(name), "")
}
