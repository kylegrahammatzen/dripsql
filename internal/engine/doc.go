// Package engine wires the parser/binder, storage, and exec layers behind an Open/Exec/Query API.
// Layout per DB. catalog.json at root, per-table manifest plus numbered segments under segments/{table}/.
package engine
