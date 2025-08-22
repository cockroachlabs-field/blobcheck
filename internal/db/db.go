// Copyright 2025 Cockroach Labs, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package db provides utilities for working with databases.
package db

import (
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cockroachdb/field-eng-powertools/stopper"
)

// Ident is a unique identifier for a database object.
type Ident string

func (i Ident) String() string {
	return string(i)
}

// Database represents a database in the system.
type Database struct {
	Name Ident
}

// Public is the public schema.
var Public = Schema{
	Name: Ident("public"),
}

// Schema represents a database schema.
type Schema struct {
	Name Ident
}

// String returns the string representation of the schema.
func (s *Schema) String() string {
	return string(s.Name)
}

const createDbStmt = `CREATE database IF NOT EXISTS %[1]s;`

// Create creates the database.
func (d *Database) Create(ctx *stopper.Context, conn *pgxpool.Conn) error {
	_, err := conn.Exec(ctx, fmt.Sprintf(createDbStmt, d.Name))
	return err
}

const dropDbStmt = `DROP database IF EXISTS %[1]s CASCADE;`

// Drop removes the database.
func (d *Database) Drop(ctx *stopper.Context, conn *pgxpool.Conn) error {
	slog.Debug("Dropping database", slog.String("database", d.Name.String()))
	_, err := conn.Exec(ctx, fmt.Sprintf(dropDbStmt, d.Name))
	return err
}

// String returns the string representation of the database.
func (d *Database) String() string {
	return string(d.Name)
}
