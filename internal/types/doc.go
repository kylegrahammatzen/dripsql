// Package types owns the runtime data shapes shared across DripSQL: Vec,
// Validity, SelectionMask, VarBytes, the kind and encoding discriminators,
// the SQL Type, and the table spec. Vec is a slim header with one
// unsafe.Pointer owning the active buffer. Validity nil means all-valid
// (Arrow convention). VarBytes uses German Strings with a 16-byte inline
// view per row. SelectionMask carries an allSet cache. Encoding wire byte
// zero is reserved for the codec-hint sentinel.
package types
