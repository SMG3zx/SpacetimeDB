package types

// Row stores a single serialized row payload keyed by row primary key.
type Row struct {
	Key  string
	Data []byte
}

// TableMutation describes inserts/deletes for one table in a transaction.
type TableMutation struct {
	Table   string
	Inserts []Row
	Deletes []string
}

// Transaction is an atomic set of table mutations.
type Transaction struct {
	Tables []TableMutation
}
