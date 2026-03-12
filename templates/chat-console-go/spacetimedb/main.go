//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen

// Package main is a SpacetimeDB chat module compiled to WebAssembly with TinyGo.
// It demonstrates tables, unique indexes, lifecycle reducers, nullable fields,
// and message validation.
//
// Run `go generate` to regenerate type-safe bindings from stdb.yaml.
// Build with: tinygo build -target wasm -o module.wasm ./
package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// ── Types ─────────────────────────────────────────────────────────────────────

// AlgebraicIdentity is the SATS type for a SpacetimeDB Identity (U256 newtype).
var AlgebraicIdentity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: strPtr("__identity__"), Type: types.AlgebraicU256},
	},
}

// AlgebraicOptionString is the SATS type for an optional (nullable) string.
var AlgebraicOptionString = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: strPtr("some"), Type: types.AlgebraicString},
		{Name: strPtr("none"), Type: types.ProductType{}},
	},
}

func strPtr(s string) *string { return &s }

// ── User ──────────────────────────────────────────────────────────────────────

// User represents a connected or previously-connected client.
type User struct {
	// Identity is the client's unique 256-bit identifier (primary key).
	Identity types.Identity
	// Name is the user's chosen display name, or nil if not yet set.
	Name *string
	// Online is true while the client has an active connection.
	Online bool
}

func encodeUser(w *bsatn.Writer, u User) {
	u.Identity.WriteBsatn(w)
	bsatn.WriteOption(w, u.Name, func(w *bsatn.Writer, s string) { w.WriteString(s) })
	w.WriteBool(u.Online)
}

func decodeUser(r *bsatn.Reader) (User, error) {
	identity, err := types.ReadIdentity(r)
	if err != nil {
		return User{}, err
	}
	name, err := bsatn.ReadOption(r, func(r *bsatn.Reader) (string, error) { return r.ReadString() })
	if err != nil {
		return User{}, err
	}
	online, err := r.ReadBool()
	if err != nil {
		return User{}, err
	}
	return User{Identity: identity, Name: name, Online: online}, nil
}

var userTable = spacetimedb.NewTableHandle("User", encodeUser, decodeUser)

// userIdentityIdx is a unique index on User.Identity for fast single-user lookups.
var userIdentityIdx = spacetimedb.NewUniqueIndex[User, types.Identity](
	"User",
	"user_identity_unique",
	func(w *bsatn.Writer, id types.Identity) { id.WriteBsatn(w) },
	encodeUser,
	decodeUser,
)

// ── Message ───────────────────────────────────────────────────────────────────

// Message represents a single chat message.
type Message struct {
	// Sender is the Identity of the user who sent the message.
	Sender types.Identity
	// Sent is the server timestamp at which the message was received.
	Sent types.Timestamp
	// Text is the message content.
	Text string
}

func encodeMessage(w *bsatn.Writer, m Message) {
	m.Sender.WriteBsatn(w)
	m.Sent.WriteBsatn(w)
	w.WriteString(m.Text)
}

func decodeMessage(r *bsatn.Reader) (Message, error) {
	sender, err := types.ReadIdentity(r)
	if err != nil {
		return Message{}, err
	}
	sent, err := types.ReadTimestamp(r)
	if err != nil {
		return Message{}, err
	}
	text, err := r.ReadString()
	if err != nil {
		return Message{}, err
	}
	return Message{Sender: sender, Sent: sent, Text: text}, nil
}

var messageTable = spacetimedb.NewTableHandle("Message", encodeMessage, decodeMessage)

// ── Module registration ───────────────────────────────────────────────────────

func init() {
	// Register the User table with a primary key and unique index on Identity.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "User",
		Columns: []spacetimedb.ColumnDef{
			{Name: "identity", Type: AlgebraicIdentity},
			{Name: "name", Type: AlgebraicOptionString},
			{Name: "online", Type: types.AlgebraicBool},
		},
		PrimaryKey: []uint16{0}, // column 0 = identity
		Indexes: []spacetimedb.IndexDef{
			{
				SourceName: strPtr("user_identity_unique"),
				Algorithm:  spacetimedb.IndexAlgorithmBTree,
				Columns:    []uint16{0},
			},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{
				SourceName: strPtr("user_identity_unique_constraint"),
				Columns:    []uint16{0},
			},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Register the Message table.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Message",
		Columns: []spacetimedb.ColumnDef{
			{Name: "sender", Type: AlgebraicIdentity},
			{Name: "sent", Type: types.AlgebraicTimestamp},
			{Name: "text", Type: types.AlgebraicString},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Register reducers.
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "SetName",
		Params: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(setNameReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "SendMessage",
		Params: []spacetimedb.ColumnDef{
			{Name: "text", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(sendMessageReducer)

	// Register lifecycle reducers.
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "ClientConnected",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterReducerHandler(clientConnectedReducer)
	spacetimedb.RegisterLifecycleDef(spacetimedb.LifecycleDef{
		Kind:    spacetimedb.LifecycleOnConnect,
		Reducer: "ClientConnected",
	})

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "ClientDisconnected",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterReducerHandler(clientDisconnectedReducer)
	spacetimedb.RegisterLifecycleDef(spacetimedb.LifecycleDef{
		Kind:    spacetimedb.LifecycleOnDisconnect,
		Reducer: "ClientDisconnected",
	})
}

// ── Reducers ──────────────────────────────────────────────────────────────────

func setNameReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		panic("SetName: failed to read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	name, err := r.ReadString()
	if err != nil {
		panic("SetName: failed to decode name: " + err.Error())
	}
	if name == "" {
		panic("Names must not be empty")
	}
	user, err := userIdentityIdx.Find(ctx.Sender)
	if err != nil {
		panic("SetName: index lookup failed: " + err.Error())
	}
	if user == nil {
		// No user found for this identity — ignore silently.
		return
	}
	user.Name = &name
	if _, err := userIdentityIdx.Update(*user); err != nil {
		panic("SetName: update failed: " + err.Error())
	}
}

func sendMessageReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		panic("SendMessage: failed to read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	text, err := r.ReadString()
	if err != nil {
		panic("SendMessage: failed to decode text: " + err.Error())
	}
	if text == "" {
		panic("Messages must not be empty")
	}
	spacetimedb.LogInfo(ctx.Sender.String() + ": " + text)
	if _, err := messageTable.Insert(Message{
		Sender: ctx.Sender,
		Sent:   ctx.Timestamp,
		Text:   text,
	}); err != nil {
		panic("SendMessage: insert failed: " + err.Error())
	}
}

func clientConnectedReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	spacetimedb.LogInfo("Connect " + ctx.Sender.String())
	user, err := userIdentityIdx.Find(ctx.Sender)
	if err != nil {
		panic("ClientConnected: index lookup failed: " + err.Error())
	}
	if user != nil {
		// Returning user — set Online: true, leave Name and Identity unchanged.
		user.Online = true
		if _, err := userIdentityIdx.Update(*user); err != nil {
			panic("ClientConnected: update failed: " + err.Error())
		}
	} else {
		// New user — insert a row with Online: true and no name yet.
		if _, err := userTable.Insert(User{
			Identity: ctx.Sender,
			Name:     nil,
			Online:   true,
		}); err != nil {
			panic("ClientConnected: insert failed: " + err.Error())
		}
	}
}

func clientDisconnectedReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	user, err := userIdentityIdx.Find(ctx.Sender)
	if err != nil {
		panic("ClientDisconnected: index lookup failed: " + err.Error())
	}
	if user != nil {
		user.Online = false
		if _, err := userIdentityIdx.Update(*user); err != nil {
			panic("ClientDisconnected: update failed: " + err.Error())
		}
	} else {
		spacetimedb.LogWarn("Warning: No user found for disconnected client.")
	}
}

// main is required by TinyGo but never called by the SpacetimeDB host.
func main() {}
