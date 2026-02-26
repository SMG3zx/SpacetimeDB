package protocol

type Row struct {
	Key  string
	Data []byte
}

type TableMutation struct {
	Table   string
	Inserts []Row
	Deletes []string
}

type Transaction struct {
	Tables []TableMutation
}

