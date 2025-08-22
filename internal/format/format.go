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

package format

import (
	"io"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/cockroachlabs-field/blobcheck/internal/validate"
)

// Report generates a report from the validation results.
func Report(w io.Writer, report *validate.Report) {
	style := table.StyleLight
	style.Format.Header = text.FormatLower
	if report.SuggestedParams != nil {
		t := table.NewWriter()
		t.SetOutputMirror(w)
		t.SetTitle("Suggested Parameters")
		t.SetStyle(style)
		t.AppendHeader(table.Row{"Parameter", "Value"})
		for k, v := range report.SuggestedParams.Iter() {
			t.AppendRow(table.Row{k, v})
		}
		t.Render()
	}
	if report.Stats != nil {
		t := table.NewWriter()
		t.SetOutputMirror(w)
		t.SetTitle("Statistics")
		t.SetStyle(style)
		t.AppendHeader(table.Row{"Node", "Read Speed", "Write Speed", "Status"})
		for _, stat := range report.Stats {
			message := "OK"
			if stat.ErrStr != "" {
				message = stat.ErrStr
			}
			t.AppendRow(table.Row{stat.Node, stat.ReadSpeed, stat.WriteSpeed, message})
		}
		t.Render()
	}
}
