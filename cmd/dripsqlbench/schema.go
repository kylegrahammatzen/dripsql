package main

import (
	"github.com/kylegrahammatzen/dripsql/internal/table"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func benchmarkSchema() []table.Column {
	return []table.Column{
		{Name: "tenant_id", Kind: vector.KindInt64},
		{Name: "user_id", Kind: vector.KindInt64},
		{Name: "created_at", Kind: vector.KindInt64},
		{Name: "event_type", Kind: vector.KindString},
		{Name: "country", Kind: vector.KindString},
		{Name: "status", Kind: vector.KindString},
		{Name: "path", Kind: vector.KindString},
		{Name: "user_agent", Kind: vector.KindString},
		{Name: "url", Kind: vector.KindString},
		{Name: "email", Kind: vector.KindString},
		{Name: "payload", Kind: vector.KindString},
	}
}

func hasColumn(schema []table.Column, name string, kind vector.Kind) bool {
	for _, col := range schema {
		if col.Name == name && col.Kind == kind {
			return true
		}
	}
	return false
}
