package types

import (
	"fmt"
	"time"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// Timestamp represents a point in time as microseconds since the Unix epoch.
// Wire format: i64 little-endian (microseconds since Unix epoch).
//
// Note: the Rust source comments say nanoseconds, but the actual SATS type
// definition uses microseconds. We store and transmit microseconds.
type Timestamp struct {
	Microseconds int64
}

// ToTime converts the Timestamp to a time.Time (UTC).
func (t Timestamp) ToTime() time.Time {
	return time.UnixMicro(t.Microseconds).UTC()
}

// TimestampFromTime creates a Timestamp from a time.Time.
func TimestampFromTime(t time.Time) Timestamp {
	return Timestamp{Microseconds: t.UnixMicro()}
}

// String formats the Timestamp as an RFC3339 string.
func (t Timestamp) String() string {
	return t.ToTime().Format(time.RFC3339Nano)
}

// WriteBsatn encodes the Timestamp as an i64 (microseconds since Unix epoch).
func (t Timestamp) WriteBsatn(w *bsatn.Writer) {
	w.WriteI64(t.Microseconds)
}

// ReadTimestamp decodes a Timestamp from an i64.
func ReadTimestamp(r *bsatn.Reader) (Timestamp, error) {
	us, err := r.ReadI64()
	if err != nil {
		return Timestamp{}, fmt.Errorf("timestamp: %w", err)
	}
	return Timestamp{Microseconds: us}, nil
}

// TimeDuration represents a span of time in nanoseconds.
// Wire format: i64 little-endian (nanoseconds).
type TimeDuration struct {
	Nanoseconds int64
}

// ToDuration converts to a time.Duration.
func (d TimeDuration) ToDuration() time.Duration {
	return time.Duration(d.Nanoseconds)
}

// TimeDurationFromDuration creates a TimeDuration from a time.Duration.
func TimeDurationFromDuration(d time.Duration) TimeDuration {
	return TimeDuration{Nanoseconds: int64(d)}
}

// String formats the TimeDuration using Go's duration format.
func (d TimeDuration) String() string {
	return d.ToDuration().String()
}

// WriteBsatn encodes the TimeDuration as an i64 (nanoseconds).
func (d TimeDuration) WriteBsatn(w *bsatn.Writer) {
	w.WriteI64(d.Nanoseconds)
}

// ReadTimeDuration decodes a TimeDuration from an i64.
func ReadTimeDuration(r *bsatn.Reader) (TimeDuration, error) {
	ns, err := r.ReadI64()
	if err != nil {
		return TimeDuration{}, fmt.Errorf("time_duration: %w", err)
	}
	return TimeDuration{Nanoseconds: ns}, nil
}

// ScheduleAt represents when a scheduled reducer should execute.
// Wire format: 1-byte variant tag followed by an i64 payload.
//   - tag 0 (Interval): i64 TimeDuration value (nanoseconds)
//   - tag 1 (Time): i64 Timestamp value (microseconds since Unix epoch)
type ScheduleAt struct {
	// IsTime is true when this is a specific time, false when it is an interval.
	IsTime bool
	// Micros holds the i64 payload: nanoseconds for Interval, microseconds for Time.
	Micros int64
}

// ScheduleAtInterval creates a ScheduleAt that fires after the given duration.
func ScheduleAtInterval(d TimeDuration) ScheduleAt {
	return ScheduleAt{IsTime: false, Micros: d.Nanoseconds}
}

// ScheduleAtTime creates a ScheduleAt that fires at the given timestamp.
func ScheduleAtTime(t Timestamp) ScheduleAt {
	return ScheduleAt{IsTime: true, Micros: t.Microseconds}
}

// WriteBsatn encodes the ScheduleAt as a BSATN sum value.
func (s ScheduleAt) WriteBsatn(w *bsatn.Writer) {
	if s.IsTime {
		w.WriteVariantTag(1)
	} else {
		w.WriteVariantTag(0)
	}
	w.WriteI64(s.Micros)
}

// ReadScheduleAt decodes a ScheduleAt from BSATN.
func ReadScheduleAt(r *bsatn.Reader) (ScheduleAt, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return ScheduleAt{}, fmt.Errorf("schedule_at: %w", err)
	}
	micros, err := r.ReadI64()
	if err != nil {
		return ScheduleAt{}, fmt.Errorf("schedule_at payload: %w", err)
	}
	return ScheduleAt{IsTime: tag == 1, Micros: micros}, nil
}
