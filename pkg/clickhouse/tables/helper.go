package tables

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func doWithTx(_ context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// asTime converts this to a time.Time.
func asTime(ts int64) time.Time {
	return time.Unix(0, ts).UTC()
}
