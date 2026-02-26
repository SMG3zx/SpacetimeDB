package protocol

const (
	WSSubprotocolV2 = "v2.bsatn.spacetimedb"
)

type Compression string

const (
	CompressionNone Compression = "None"
	CompressionGzip Compression = "Gzip"
)
