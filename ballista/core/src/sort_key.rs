// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Sketching a single fixed-width `ORDER BY` key.
//!
//! [`crate::sort_key::SortKeyCodec`] is the ordering spec for one key —
//! its type, its direction, where its NULLs go — and encodes values to an
//! order-preserving `u64` and back. [`crate::sort_key::SortKeySketch`]
//! pairs that with a [`crate::kll::KllSketch`] over the encoded values and
//! a count of the NULLs, and answers quantiles over the whole population.
//!
//! Consumers want the sketch, not the codec. It is the type that knows how
//! to merge two observations, how a NULL run shifts a quantile, and what
//! goes on the wire.
//!
//! # Why an integer key
//!
//! The sketch needs `T: Ord`, and the obvious candidates for an `ORDER BY`
//! column are a type-specific wrapper (`OrderedFloat<f64>` and friends) or
//! the arrow row format. Both were measured against this encoding in
//! `benchmarks/benches/quantile_sketch.rs`; at n=1M, ratios to the
//! incumbent T-Digest are 1.23× for this encoding, 1.80× for
//! `OrderedFloat<f64>`, and 3.75× for arrow-row bytes held inline.
//!
//! Ingest cost is dominated by the `sort_unstable` inside KLL's compaction,
//! so the comparator is what matters: a `u64` compare is one instruction,
//! where a float total order is bit manipulation plus branches and
//! arrow-row pays ~25 ns/row to encode in the first place. Collapsing every
//! fixed-width type to a plain integer therefore wins on speed as well as
//! on uniformity.
//!
//! It also keeps sort direction out of the type system. `DESC` is a
//! bitwise NOT of the key rather than a second `Ord` implementation, so one
//! sketch type serves both directions instead of one per combination.
//!
//! # NULLs are out of band
//!
//! Encoding skips NULLs entirely and `SortKeySketch` counts them instead.
//! A NULL has no position among the values, only a side, and `nulls_first`
//! / `nulls_last` says which — that is one bit of plan-time information,
//! not something the key needs to carry. Keeping it out leaves the key 8
//! bytes wide rather than 16, which the same benchmark measured at 1.23×
//! versus 1.58×.
//!
//! The cost is that a rank over the population is no longer a rank over
//! the values: the NULL run has to be stepped over first. That remap lives
//! in [`crate::sort_key::SortKeySketch::quantile`] and nowhere else.
//! Spread across call sites it would be reimplemented per consumer, and
//! getting it wrong skews every cut without failing anything.
//!
//! # The row format is the rulebook
//!
//! Arrow's row format is the only complete statement of what a SQL
//! `ORDER BY` means: it folds the column type, `nulls_first`, and
//! `descending` into a single memcmp order, and arrow's own sort agrees
//! with it. So it defines the answer, and anything faster is only allowed
//! to be an implementation of that answer.
//!
//! This encoding is exactly that. Its float transform is the same one
//! `arrow_row::fixed` applies, and both reduce to `total_cmp`, which is
//! what `ArrowNativeTypeOp::compare` uses. The test
//! `integer_keys_order_identically_to_arrow_row` pins the agreement on a
//! fixture containing ±NaN, ±0.0 and both infinities, so a divergence fails
//! a test rather than surfacing as misrouted rows.
//!
//! Following the rulebook is also what makes NaN a non-event. NaN has a
//! defined place in `total_cmp` — beyond the infinity of its own sign — so
//! it becomes an ordinary key, at the top or bottom of the `u64` range.
//! Comparisons against it behave, and this module contains no NaN handling
//! whatsoever. Code that compares raw `f64` instead has to special-case it,
//! because `partial_cmp` answers "no" to every question a router asks.
//!
//! # Exactness
//!
//! Every encoding here is a bijection on its type's value range, so a
//! quantile drawn from the sketch converts back to the precise value it
//! came from — not an approximation of it. That is what lets a
//! `Timestamp(Nanosecond)` cut stay nanosecond-exact; casting through
//! `f64` would round it to a 256 ns grid at 2020s epoch magnitudes, since
//! those sit above `f64`'s 2^53 integer limit.
//!
//! Where that exactness is worth something is narrower than it looks, and
//! worth stating so nobody over-claims it. It is not the quantiles: those
//! carry the sketch's own rank error, which on a uniform 1M stream is
//! ~0.2%, and 0.2% of a partition covering one day is about three minutes.
//! A 256 ns rounding is nine orders of magnitude beneath that. Any
//! argument resting on quantile precision is noise.
//!
//! It is the extremes. `min` and `max` are exact by construction, tracked
//! outside the compactor so no coin flip can move them, and `cut_partitions`
//! routes shuffle files on exactly those two values. There the error bars
//! are zero, so anything a cast rounds away is error introduced where none
//! existed. Keys compare with `Ord` over every element, which has no value
//! it silently ignores.
//!
//! The other case is a narrow spread at a large magnitude, since float
//! precision is relative: a partition spanning a day is unaffected, one
//! spanning 100 µs at 2020s epoch nanos is past the point where the cast
//! costs more than the sketch does.
//!
//! # Coverage
//!
//! Signed and unsigned integers, `Float32`/`Float64`, and the temporal
//! types that are `i32` or `i64` underneath (`Date`, `Time`, `Timestamp`,
//! `Duration`). [`crate::sort_key::SortKeyCodec::try_new`] returns `None`
//! for anything else
//! — `Decimal128` and wider don't fit in `u64`, `Interval` has no total
//! order, and variable-width types have no fixed encoding — leaving those
//! to the arrow-row path.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, ListArray, PrimitiveArray, RecordBatch,
    StructArray, new_empty_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::datatypes::{Field, Fields, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{Result, ScalarValue, internal_datafusion_err, internal_err};

use crate::kll::KllSketch;
use crate::serde::protobuf::SortKeySketchState;

/// Field name for the one list-of-levels column in a serialized sketch.
const LEVELS_FIELD_NAME: &str = "levels";
/// Field name of the single key column inside each item struct.
///
/// The item is a struct so a multi-column key is siblings alongside it rather
/// than a second payload shape. What holds that back is this module's key,
/// not the format: a [`SortKeyCodec`] encodes one column to one `u64`. Adding
/// columns means teaching the encode side to emit them *and* relaxing
/// [`SortKeySketch::try_from_proto`], which refuses anything but one field so
/// a half-widened producer fails loudly instead of silently dropping a key.
const KEY_FIELD_NAME: &str = "expr_0";

/// Bijection between a primitive's native value and a `u64` whose ascending
/// order matches the native ascending order.
trait SortableNative: Copy {
    /// Map to the ascending `u64` key space.
    fn to_key(self) -> u64;
    /// Inverse of [`Self::to_key`], exact for any key that method produced.
    fn from_key(key: u64) -> Self;
}

/// Signed integers: flipping the sign bit maps the two's-complement order
/// onto unsigned order, because it slides the negative half below the
/// positive half. Narrower widths sign-extend to `i64` first, which
/// preserves order within their range.
macro_rules! impl_sortable_signed {
    ($native:ty) => {
        impl SortableNative for $native {
            fn to_key(self) -> u64 {
                (self as i64 as u64) ^ (1 << 63)
            }
            fn from_key(key: u64) -> Self {
                (key ^ (1 << 63)) as i64 as Self
            }
        }
    };
}

/// Unsigned integers are already in key order; widening preserves it.
macro_rules! impl_sortable_unsigned {
    ($native:ty) => {
        impl SortableNative for $native {
            fn to_key(self) -> u64 {
                self as u64
            }
            fn from_key(key: u64) -> Self {
                key as Self
            }
        }
    };
}

/// IEEE-754 floats.
///
/// This is a permutation, not a packing: 64 bits in, 64 bits out, nothing
/// compressed and nothing lost, which is why it inverts exactly.
///
/// The layout was designed to almost sort as an integer already.
///
/// ```text
///  63   62            52   51                                      0
/// ┌────┬─────────────────┬──────────────────────────────────────────┐
/// │ S  │    exponent     │                mantissa                  │
/// │ 1  │       11        │                   52                     │
/// └────┴─────────────────┴──────────────────────────────────────────┘
///   ^          ^                            ^
///   │          │                            └── low bits
///   │          └── high bits, right below the sign
///   └── 0 = positive, 1 = negative
/// ```
///
/// The exponent sits *above* the mantissa deliberately. Compare two
/// same-sign floats as plain integers and the exponent dominates while the
/// mantissa breaks ties, which is exactly magnitude order. The bits already
/// sort themselves. Two things are wrong with them:
///
/// ```text
/// as raw unsigned integers:
///
///   0x0000...  +0.0 ─┐
///   0x3FF0...  +1.0  │  positives: right order, stuck at the BOTTOM
///   0x7FF0...  +inf ─┘
///   0x8000...  -0.0 ─┐
///   0xBFF0...  -1.0  │  negatives: at the TOP, and running BACKWARDS
///   0xFFF0...  -inf ─┘
///
/// problem 1: a set sign bit makes negatives look huge
/// problem 2: within negatives, bigger magnitude = bigger integer
/// ```
///
/// One branch on the sign bit fixes both:
///
/// ```text
/// sign bit 0 (non-negative):  key = bits ^ 0x8000000000000000
///                                   └─ flip only the sign bit, moving
///                                      them to the TOP half; the order
///                                      among them is untouched
///
/// sign bit 1 (negative):      key = !bits
///                                   └─ flip every bit: sign 1→0 moves
///                                      them to the BOTTOM half, and
///                                      inverting the rest reverses their
///                                      order, which is problem 2's fix
/// ```
///
/// What comes out the other end:
///
/// ```text
///    value      f64 bits             key (u64)            order
///   ───────────────────────────────────────────────────────────
///    -NaN     0xFFF8000000000000    0x0007FFFFFFFFFFFF     ▲ smallest
///    -inf     0xFFF0000000000000    0x000FFFFFFFFFFFFF     │
///    -2.0     0xC000000000000000    0x3FFFFFFFFFFFFFFF     │
///    -1.0     0xBFF0000000000000    0x400FFFFFFFFFFFFF     │
///    -0.0     0x8000000000000000    0x7FFFFFFFFFFFFFFF     │
///    +0.0     0x0000000000000000    0x8000000000000000     │
///    +1.0     0x3FF0000000000000    0xBFF0000000000000     │
///    +2.0     0x4000000000000000    0xC000000000000000     │
///    +inf     0x7FF0000000000000    0xFFF0000000000000     │
///    +NaN     0x7FF8000000000000    0xFFF8000000000000     ▼ largest
/// ```
///
/// That is `f64::total_cmp` order, which is what arrow sorts by.
///
/// NaN needed no work. Its exponent is all ones with a nonzero mantissa,
/// so its pattern sits just above the infinity on its own side, which has
/// the same exponent and a zero mantissa. It lands past infinity by
/// itself. Nothing here tests for it: NaN is only awkward when compared
/// *as a float*.
///
/// Note that all 2^64 keys are spoken for, so there is no spare slot to
/// mean NULL. That would need a 65th bit, and in practice a 16-byte key —
/// which is why NULLs are counted out of band instead. See the module
/// docs.
macro_rules! impl_sortable_float {
    ($native:ty, $bits:ty, $width:expr) => {
        impl SortableNative for $native {
            fn to_key(self) -> u64 {
                let bits = self.to_bits();
                let sign: $bits = 1 << ($width - 1);
                let key: $bits = if bits & sign != 0 { !bits } else { bits ^ sign };
                // Zero-extending a narrower key preserves order, since
                // every key of that width is below the widened range.
                key as u64
            }
            fn from_key(key: u64) -> Self {
                let bits = key as $bits;
                let sign = 1 << ($width - 1);
                // Forward maps negatives to a cleared top bit and
                // non-negatives to a set one, so the top bit selects the
                // branch to undo.
                let bits = if bits & sign != 0 { bits ^ sign } else { !bits };
                Self::from_bits(bits)
            }
        }
    };
}

impl_sortable_signed!(i8);
impl_sortable_signed!(i16);
impl_sortable_signed!(i32);
impl_sortable_signed!(i64);
impl_sortable_unsigned!(u8);
impl_sortable_unsigned!(u16);
impl_sortable_unsigned!(u32);
impl_sortable_unsigned!(u64);
impl_sortable_float!(f32, u32, 32);
impl_sortable_float!(f64, u64, 64);

/// Invoke `$handler!(ArrowPrimitiveType)` for the arrow type backing
/// `$data_type`, or evaluate `$fallback` when it isn't one this module
/// encodes.
///
/// This allowlist *is* the tier boundary: every type named here gets the
/// `u64` fast path, and everything omitted falls through to arrow-row.
/// Adding a type means adding it here and nowhere else.
macro_rules! dispatch_sortable {
    ($data_type:expr, $handler:ident, $fallback:expr) => {
        match $data_type {
            DataType::Int8 => $handler!(Int8Type),
            DataType::Int16 => $handler!(Int16Type),
            DataType::Int32 => $handler!(Int32Type),
            DataType::Int64 => $handler!(Int64Type),
            DataType::UInt8 => $handler!(UInt8Type),
            DataType::UInt16 => $handler!(UInt16Type),
            DataType::UInt32 => $handler!(UInt32Type),
            DataType::UInt64 => $handler!(UInt64Type),
            DataType::Float32 => $handler!(Float32Type),
            DataType::Float64 => $handler!(Float64Type),
            DataType::Date32 => $handler!(Date32Type),
            DataType::Date64 => $handler!(Date64Type),
            DataType::Time32(TimeUnit::Second) => $handler!(Time32SecondType),
            DataType::Time32(TimeUnit::Millisecond) => $handler!(Time32MillisecondType),
            DataType::Time64(TimeUnit::Microsecond) => $handler!(Time64MicrosecondType),
            DataType::Time64(TimeUnit::Nanosecond) => $handler!(Time64NanosecondType),
            DataType::Timestamp(TimeUnit::Second, _) => $handler!(TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                $handler!(TimestampMillisecondType)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                $handler!(TimestampMicrosecondType)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                $handler!(TimestampNanosecondType)
            }
            DataType::Duration(TimeUnit::Second) => $handler!(DurationSecondType),
            DataType::Duration(TimeUnit::Millisecond) => {
                $handler!(DurationMillisecondType)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                $handler!(DurationMicrosecondType)
            }
            DataType::Duration(TimeUnit::Nanosecond) => $handler!(DurationNanosecondType),
            _ => $fallback,
        }
    };
}

/// The complete ordering spec for one fixed-width `ORDER BY` key: its
/// type, its direction, and where its NULLs go. Encodes values to `u64`
/// and back. See the module docs for the encoding and for why NULLs are
/// handled out of band.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKeyCodec {
    /// The column's full arrow type, retained so [`Self::decode`] can
    /// rebuild a `ScalarValue` that keeps the parts the key doesn't carry
    /// — a `Timestamp`'s timezone above all.
    data_type: DataType,
    /// `descending` inverts every key bit, reversing the sketch's ascending
    /// order into the order the plan asked for. `nulls_first` never touches
    /// a key, since NULLs are not encoded; it tells a sketch which end of
    /// the distribution its NULL count occupies.
    options: SortOptions,
}

impl SortKeyCodec {
    /// Build a codec for `data_type` under `options`, or `None` if this
    /// module doesn't encode that type and the caller should fall back to
    /// arrow-row.
    pub fn try_new(data_type: &DataType, options: SortOptions) -> Option<Self> {
        macro_rules! supported {
            ($arrow_type:ty) => {
                true
            };
        }
        let supported = dispatch_sortable!(data_type, supported, false);
        supported.then(|| Self {
            data_type: data_type.clone(),
            options,
        })
    }

    /// The arrow type this codec was built for.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// The sort direction and NULL placement this codec encodes for.
    pub fn options(&self) -> SortOptions {
        self.options
    }

    /// A typed NULL of this codec's column type. What a quantile query
    /// answers when the rank it asks for lands in the NULL run.
    pub fn null_value(&self) -> Result<ScalarValue> {
        ScalarValue::try_from(&self.data_type)
    }

    /// Encode `array`'s non-NULL values in row order.
    ///
    /// NULLs are skipped, so the result is shorter than `array` by exactly
    /// `array.null_count()` — callers that need that count read it from the
    /// array. The output is ready for `KllSketch::absorb_slice`.
    ///
    /// Errors if `array`'s type doesn't match the one this codec was built
    /// for, which would mean the routing expression changed type between
    /// planning and execution.
    ///
    /// The match is exact `DataType` equality, so a codec built for
    /// `Timestamp(ns, Some("UTC"))` rejects an array tagged `Some("+00:00")`
    /// even though the two encode to identical keys. Strict because the
    /// codec's type is what [`Self::decode`] rebuilds a `ScalarValue` from,
    /// so accepting an array of a type the codec doesn't carry would
    /// relabel every cut it later produces.
    pub fn encode(&self, array: &dyn Array) -> Result<Vec<u64>> {
        if array.data_type() != &self.data_type {
            return Err(internal_datafusion_err!(
                "SortKeyCodec: built for {:?} but got {:?}",
                self.data_type,
                array.data_type()
            ));
        }
        macro_rules! encode_as {
            ($arrow_type:ty) => {{
                let typed = array.as_primitive_opt::<$arrow_type>().ok_or_else(|| {
                    internal_datafusion_err!(
                        "SortKeyCodec: {:?} array failed to downcast to its own \
                         primitive type",
                        self.data_type
                    )
                })?;
                Ok(self.encode_primitive(typed))
            }};
        }
        dispatch_sortable!(
            &self.data_type,
            encode_as,
            Err(internal_datafusion_err!(
                "SortKeyCodec: {:?} is not encodable — try_new should have \
                 returned None",
                self.data_type
            ))
        )
    }

    /// Shared body of every [`Self::encode`] arm, monomorphized per arrow
    /// type. Split out so the all-non-NULL case can read the values buffer
    /// directly instead of going through the nullable iterator.
    fn encode_primitive<T>(&self, array: &PrimitiveArray<T>) -> Vec<u64>
    where
        T: ArrowPrimitiveType,
        T::Native: SortableNative,
    {
        let descending = self.options.descending;
        let orient = move |key: u64| if descending { !key } else { key };
        if array.null_count() == 0 {
            array.values().iter().map(|v| orient(v.to_key())).collect()
        } else {
            array.iter().flatten().map(|v| orient(v.to_key())).collect()
        }
    }

    /// Recover the value a key came from, as a `ScalarValue` carrying this
    /// codec's full arrow type.
    ///
    /// Exact for any key [`Self::encode`] produced. A key from anywhere else
    /// still decodes — the map is total — but to an arbitrary value of the
    /// type.
    pub fn decode(&self, key: u64) -> Result<ScalarValue> {
        // Bitwise NOT is an involution, so the same branch undoes DESC.
        let key = if self.options.descending { !key } else { key };
        macro_rules! decode_as {
            ($arrow_type:ty) => {{
                let native =
                    <<$arrow_type as ArrowPrimitiveType>::Native as SortableNative>::from_key(key);
                ScalarValue::new_primitive::<$arrow_type>(Some(native), &self.data_type)
            }};
        }
        dispatch_sortable!(
            &self.data_type,
            decode_as,
            Err(internal_datafusion_err!(
                "SortKeyCodec: {:?} is not decodable — try_new should have \
                 returned None",
                self.data_type
            ))
        )
    }
}

/// KLL top-level compactor capacity. Picked for worst-case rank-error
/// parity with the T-Digest sizing it replaces (`max_size = 100`), so the
/// swap changes the sketch's cost and exactness without changing its
/// accuracy: on a uniform 1M stream, 0.0016 worst-case normalized rank
/// error against T-Digest's 0.0021. Rerun with `KLL_PARITY_CHECK=1 cargo
/// bench --bench quantile_sketch`.
pub const KLL_K: usize = 800;

/// One `ORDER BY` key's observed distribution: a quantile sketch over the
/// non-NULL values, plus the count of the NULLs that have no place in it.
///
/// Holding both together is the point. NULLs sit at one end of the order
/// rather than among the values, so any quantile over the *population*
/// has to account for the NULL run before consulting the sketch. Doing
/// that remap at call sites would mean every consumer reimplementing it,
/// and getting it wrong skews cuts silently rather than failing. So merge,
/// quantile, and the wire format all live here, once.
#[derive(Debug, Clone)]
pub struct SortKeySketch {
    /// How values become keys, and which end the NULLs occupy.
    codec: SortKeyCodec,
    /// Quantile structure over the non-NULL values only.
    sketch: KllSketch<u64>,
    /// Rows whose key was NULL. Not in `sketch`, and not recoverable from
    /// it.
    null_count: u64,
}

impl SortKeySketch {
    /// An empty sketch for the key `codec` describes.
    pub fn new(codec: SortKeyCodec) -> Self {
        Self {
            codec,
            sketch: KllSketch::new(KLL_K),
            null_count: 0,
        }
    }

    /// Observe every row of `array`: encode the non-NULL values into the
    /// sketch and add the NULLs to the count.
    ///
    /// Errors if `array`'s type disagrees with the codec's, which would
    /// mean the routing expression changed type between planning and
    /// execution.
    pub fn ingest(&mut self, array: &dyn Array) -> Result<()> {
        let keys = self.codec.encode(array)?;
        // Sketching a sort key usually means sitting above the `SortExec`
        // that produced it, so the sorted path is the common case rather
        // than a special one. It verifies sortedness in O(n) comparisons and
        // falls through when the input isn't sorted, so it is correct to
        // call unconditionally — a `DESC` key, whose encoding inverts every
        // bit and so arrives descending, takes that fallback.
        self.sketch.absorb_sorted_slice(&keys);
        self.null_count += array.null_count() as u64;
        Ok(())
    }

    /// Fold `other` into `self`.
    ///
    /// Errors when the two describe different keys. Merging a sketch of
    /// one column into a sketch of another produces a plausible-looking
    /// distribution of nothing in particular, so it is caught rather than
    /// tolerated.
    pub fn merge(&mut self, other: Self) -> Result<()> {
        if self.codec != other.codec {
            return Err(internal_datafusion_err!(
                "SortKeySketch::merge: {:?} and {:?} describe different sort keys",
                self.codec,
                other.codec
            ));
        }
        self.sketch.merge(other.sketch);
        self.null_count += other.null_count;
        Ok(())
    }

    /// Rows observed, NULLs included.
    pub fn count(&self) -> u64 {
        self.sketch.count() + self.null_count
    }

    /// Rows observed whose key was NULL.
    pub fn null_count(&self) -> u64 {
        self.null_count
    }

    /// The ordering spec these observations were made under.
    pub fn codec(&self) -> &SortKeyCodec {
        &self.codec
    }

    /// The least value in sort order, or `None` when nothing was observed.
    ///
    /// A typed NULL when NULLs sort first and at least one was seen, since
    /// then the least *row* is a NULL rather than a value.
    pub fn min(&self) -> Result<Option<ScalarValue>> {
        self.extreme(self.codec.options().nulls_first, self.sketch.min())
    }

    /// The greatest value in sort order, or `None` when nothing was
    /// observed. A typed NULL when NULLs sort last and at least one was
    /// seen.
    pub fn max(&self) -> Result<Option<ScalarValue>> {
        self.extreme(!self.codec.options().nulls_first, self.sketch.max())
    }

    /// The least value observed, or `None` when no value was.
    ///
    /// Distinct from [`Self::min`], which answers about the least *row* and so
    /// reports a typed NULL when NULLs sort first. A router comparing a key
    /// against a range wants this one: a NULL bound makes its comparison NULL
    /// and drops every row it was meant to select.
    pub fn value_min(&self) -> Result<Option<ScalarValue>> {
        self.sketch
            .min()
            .map(|key| self.codec.decode(*key))
            .transpose()
    }

    /// The greatest value observed, or `None` when no value was. Counterpart
    /// to [`Self::value_min`].
    pub fn value_max(&self) -> Result<Option<ScalarValue>> {
        self.sketch
            .max()
            .map(|key| self.codec.decode(*key))
            .transpose()
    }

    /// Shared body of [`Self::min`] and [`Self::max`]: the extreme is a
    /// NULL when the NULL run is on `nulls_are_on_this_end` and non-empty,
    /// otherwise it is `value_extreme` decoded.
    fn extreme(
        &self,
        nulls_are_on_this_end: bool,
        value_extreme: Option<&u64>,
    ) -> Result<Option<ScalarValue>> {
        // With no values at all the run is unbounded on both sides, so the
        // end it does not nominally occupy is a NULL too. Answering `None`
        // there would claim nothing was observed while `count` says
        // otherwise, and would hand `cut_partitions` half a range.
        if self.null_count > 0 && (nulls_are_on_this_end || value_extreme.is_none()) {
            return Ok(Some(self.codec.null_value()?));
        }
        value_extreme.map(|key| self.codec.decode(*key)).transpose()
    }

    /// The value at the `q`-quantile of everything observed, NULLs
    /// included. `q` is clamped to `[0, 1]`.
    ///
    /// `Ok(None)` means nothing was observed at all. `Ok(Some(null))` means
    /// the rank `q` asks for lands inside the NULL run, which is a real
    /// answer: a cut there says the partition below it holds only NULLs.
    ///
    /// # The remap
    ///
    /// NULLs occupy a contiguous run at one end, so a rank over the
    /// population maps onto a rank over the values by subtracting the run
    /// when it sits below, and by nothing when it sits above:
    ///
    /// ```text
    ///  nulls_first        nulls_last
    ///  ┌────────┬─────┐   ┌─────┬────────┐
    ///  │ NULLs  │ vals│   │ vals│ NULLs  │
    ///  └────────┴─────┘   └─────┴────────┘
    ///   0      n     N     0    v        N
    ///
    ///  rank = (q · N) as integer
    ///  rank <= n  -> NULL          rank <= v -> values, at rank
    ///  else       -> values,       else      -> NULL
    ///                at rank - n
    /// ```
    ///
    /// The subtraction stays in integers on purpose. Rescaling the rank
    /// into a fraction of the value run and letting the sketch multiply it
    /// back loses it: with 99 values, rank 59 becomes `59/99`, and `59/99 ·
    /// 99` is `58.999…`, which truncates to 58. That off-by-one was caught
    /// by `quantiles_match_the_nulls_in_key_oracle`.
    pub fn quantile(&self, q: f64) -> Result<Option<ScalarValue>> {
        if self.count() == 0 {
            return Ok(None);
        }
        match self.value_rank(q) {
            None => Ok(Some(self.codec.null_value()?)),
            Some(rank) => self
                .sketch
                .at_rank(rank)
                .map(|key| self.codec.decode(*key))
                .transpose(),
        }
    }

    /// The rank *among the values* that population-quantile `q` asks for,
    /// or `None` when it lands inside the NULL run.
    fn value_rank(&self, q: f64) -> Option<u64> {
        let rank = (q.clamp(0.0, 1.0) * self.count() as f64) as u64;
        // No NULL run to step over, so the population rank *is* the value
        // rank. This is also the only path that reaches the sketch's
        // exact-extreme handling at q = 0 and q = 1.
        if self.null_count == 0 {
            return Some(rank);
        }
        let values = self.sketch.count();
        if values == 0 {
            return None;
        }
        if self.codec.options().nulls_first {
            (rank > self.null_count).then(|| rank - self.null_count)
        } else {
            (rank <= values).then_some(rank)
        }
    }

    /// The `partitions - 1` boundaries splitting everything observed into
    /// `partitions` runs of equal size, in sort order. If a cut _would be_ NULL, it is adjusted to
    /// include the nearest value, so this function never returns a NULL cut.
    ///
    /// The run is indivisible, so it takes the partition at its end whole and
    /// only the values beside it balance.
    ///
    /// ```text
    /// nulls_first                        nulls_last
    /// ┌──────┬─────────────────┐         ┌─────────────────┬──────┐
    /// │NULLs │ values          │         │ values          │NULLs │
    /// └──────┴─────────────────┘         └─────────────────┴──────┘
    /// 0      n                 N         0                 v      N
    ///
    /// rank = max(pop − n, ...)           rank = min(pop, ...)
    /// pulls UP from min                  pulls DOWN from max
    /// ```
    ///
    /// | nulls_first | NULLs | values | K | cuts           | partition sizes   |
    /// |-------------|------:|-------:|--:|----------------|-------------------|
    /// | true        |    10 | 1..=90 | 4 | `[15, 40, 65]` | 24 / 25 / 25 / 26 |
    /// | true        |    60 | 1..=40 | 4 | `[1, 14, 27]`  | 60 / 13 / 13 / 14 |
    /// | false       |    60 | 1..=40 | 4 | `[14, 27, 40]` | 13 / 13 / 13 / 61 |
    /// | false       |    90 | 1..=10 | 4 | `[4, 7, 10]`   | 3 / 3 / 3 / 91    |
    ///
    /// Empty when `partitions < 2` or no value was observed.
    ///
    /// Should only error if:
    /// 1. invalid sketch: min/max is empty but levels are not - guarded against in proto decode
    /// 2. codec.decode() failure - guarded against in try_new
    pub fn cuts(&self, partition_cnt: usize) -> Result<Vec<ScalarValue>> {
        if partition_cnt < 2 || self.sketch.count() == 0 {
            return Ok(Vec::new());
        }
        let total_cnt = self.count();
        let sketch_cnt = self.sketch.count();
        // Partitions sharing the not-NULLs: all but the one partition consumed by NULLs
        let sharing = partition_cnt as u128 - 1;
        // null_count > total_cnt / partition_cnt (but without error inducing division)
        let nulls_outgrow_a_partition =
            self.null_count as u128 * partition_cnt as u128 > total_cnt as u128;
        let sketch_ranks: Vec<u64> = (0..partition_cnt - 1)
            .map(|cut_idx| {
                let total_rank =
                    ((cut_idx as u128 + 1) * total_cnt as u128 / partition_cnt as u128) as u64;
                if self.codec.options().nulls_first {
                    let sketch_rank = if !nulls_outgrow_a_partition {
                        // no cut ever lands on a NULL value anyway
                        total_rank.saturating_sub(self.null_count)
                    } else {
                        // save a whole partition for the NULLs, divide evenly amongst the rest
                        (cut_idx as u128 * sketch_cnt as u128 / sharing) as u64 + 1
                    };
                    sketch_rank.max(cut_idx as u64 + 1)
                } else {
                    // mirror of above
                    let sketch_rank = if !nulls_outgrow_a_partition {
                        total_rank
                    } else {
                        ((cut_idx as u128 + 1) * sketch_cnt as u128).div_ceil(sharing) as u64
                    };
                    let distinct = sketch_cnt
                        .saturating_sub(partition_cnt as u64 - 2 - cut_idx as u64)
                        .max(1);
                    sketch_rank.min(distinct).max(1)
                }
            })
            .collect();

        // One sorted pass for every boundary. Per-cut `quantile` re-sorts the
        // retained set each time: 279 us against 7.56 us at K=64.
        self.sketch
            .at_ranks(&sketch_ranks)
            .into_iter()
            .zip(&sketch_ranks)
            .map(|(key, rank)| {
                let key = key.ok_or_else(|| {
                    internal_datafusion_err!(
                        "SortKeySketch: no value at rank {rank} of {} values",
                        self.sketch.count()
                    )
                })?;
                self.codec.decode(*key)
            })
            .collect()
    }

    /// Serialize to [`SortKeySketchState`]. See that message for the layout
    /// and for what was priced against it.
    ///
    /// The key's direction and NULL placement are deliberately absent: they
    /// live once per report, in the `order_by` tag every consumer already
    /// reads to know which expression a sketch describes.
    pub fn to_proto(&self) -> Result<SortKeySketchState> {
        let mut offsets: Vec<i32> = Vec::with_capacity(self.sketch.levels().len() + 1);
        offsets.push(0);
        let mut keys: Vec<u64> = Vec::new();
        for level in self.sketch.levels() {
            let mut ascending = level.clone();
            ascending.sort_unstable();
            keys.extend_from_slice(&ascending);
            offsets.push(i32::try_from(keys.len()).map_err(|_| {
                internal_datafusion_err!(
                    "SortKeySketch: {} retained items overflow an arrow list offset",
                    keys.len()
                )
            })?);
        }

        let item = Arc::new(Field::new(
            KEY_FIELD_NAME,
            self.codec.data_type().clone(),
            false,
        ));
        let items = StructArray::try_new(
            Fields::from(vec![item]),
            vec![self.decode_all(&keys)?],
            None,
        )?;
        let levels = ListArray::try_new(
            Arc::new(Field::new("item", items.data_type().clone(), false)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(items),
            None,
        )?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            LEVELS_FIELD_NAME,
            levels.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(levels)])?;

        let mut ipc = Vec::new();
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);

        Ok(SortKeySketchState {
            k: u32::try_from(self.sketch.k()).map_err(|_| {
                internal_datafusion_err!(
                    "SortKeySketch: k={} exceeds u32",
                    self.sketch.k()
                )
            })?,
            null_count: self.null_count,
            key_min: self.extreme_proto(self.sketch.min())?,
            key_max: self.extreme_proto(self.sketch.max())?,
            levels: ipc,
        })
    }

    /// Rebuild what [`Self::to_proto`] wrote. `options` comes from the
    /// report's `order_by` tag; the key's type comes from the payload's own
    /// arrow schema, so the two together reconstruct the codec.
    ///
    /// Errors on a payload this sketch could not have produced — a schema
    /// that isn't one list of structs, or a compactor stack over capacity —
    /// rather than returning a sketch whose answers would be quietly wrong.
    pub fn try_from_proto(
        proto: &SortKeySketchState,
        options: SortOptions,
    ) -> Result<Self> {
        let mut reader =
            StreamReader::try_new(std::io::Cursor::new(&proto.levels), None)?;
        let batch = reader.next().transpose()?.ok_or_else(|| {
            internal_datafusion_err!("SortKeySketchState: levels payload holds no batch")
        })?;
        let levels = batch
            .column_by_name(LEVELS_FIELD_NAME)
            .and_then(|column| column.as_list_opt::<i32>())
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "SortKeySketchState: expected a `{LEVELS_FIELD_NAME}` list column, got {:?}",
                    batch.schema()
                )
            })?;

        let key_type = match levels.value_type() {
            DataType::Struct(fields) => match fields.as_ref() {
                [only] => only.data_type().clone(),
                other => {
                    return internal_err!(
                        "SortKeySketchState: expected one key field per item, got {}",
                        other.len()
                    );
                }
            },
            other => {
                return internal_err!(
                    "SortKeySketchState: expected items to be structs, got {other:?}"
                );
            }
        };
        let codec = SortKeyCodec::try_new(&key_type, options).ok_or_else(|| {
            internal_datafusion_err!(
                "SortKeySketchState: {key_type:?} is not an encodable key"
            )
        })?;

        let mut stack: Vec<Vec<u64>> = Vec::with_capacity(levels.len());
        for level in 0..levels.len() {
            let items = levels.value(level);
            let keys = items
                .as_struct_opt()
                .map(|items| codec.encode(items.column(0).as_ref()))
                .transpose()?
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "SortKeySketchState: level {level} is not a struct array"
                    )
                })?;
            stack.push(keys);
        }

        let key_min = Self::extreme_key(&codec, &proto.key_min)?;
        let key_max = Self::extreme_key(&codec, &proto.key_max)?;
        // The extremes are set on the first insert and never cleared, so a
        // stack holding keys has both and an empty one has neither. `cuts`
        // reads a rank off an extreme and has no answer when it is missing.
        let has_keys = stack.iter().any(|level| !level.is_empty());
        if has_keys != key_min.is_some() || has_keys != key_max.is_some() {
            return internal_err!(
                "SortKeySketchState: {} retained keys against key_min={} key_max={} — \
                 a stack holding keys carries both extremes",
                stack.iter().map(Vec::len).sum::<usize>(),
                proto.key_min.len(),
                proto.key_max.len()
            );
        }

        let sketch = KllSketch::from_parts(proto.k as usize, stack, key_min, key_max)
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "SortKeySketchState: k={} and the level widths describe a stack KLL \
                 could not have produced",
                    proto.k
                )
            })?;
        Ok(Self {
            codec,
            sketch,
            null_count: proto.null_count,
        })
    }

    /// Every key decoded into one array of the codec's type, in the order
    /// given. Its own function because `iter_to_array` refuses an empty
    /// iterator, and a sketch that observed nothing still serializes.
    fn decode_all(&self, keys: &[u64]) -> Result<ArrayRef> {
        if keys.is_empty() {
            return Ok(new_empty_array(self.codec.data_type()));
        }
        ScalarValue::iter_to_array(
            keys.iter()
                .map(|key| self.codec.decode(*key))
                .collect::<Result<Vec<_>>>()?,
        )
    }

    /// One extreme as the wire's `repeated ScalarValue`: a tuple with one
    /// element per key column, empty when nothing was observed.
    fn extreme_proto(
        &self,
        key: Option<&u64>,
    ) -> Result<Vec<datafusion_proto_common::ScalarValue>> {
        key.map(|key| {
            let value = self.codec.decode(*key)?;
            datafusion_proto_common::ScalarValue::try_from(&value).map_err(|e| {
                internal_datafusion_err!(
                    "SortKeySketch: failed to encode {value:?}: {e:?}"
                )
            })
        })
        .transpose()
        .map(|encoded| encoded.into_iter().collect())
    }

    /// Reverses [`Self::extreme_proto`].
    fn extreme_key(
        codec: &SortKeyCodec,
        proto: &[datafusion_proto_common::ScalarValue],
    ) -> Result<Option<u64>> {
        let [value] = proto else {
            return match proto {
                [] => Ok(None),
                other => internal_err!(
                    "SortKeySketchState: expected one element per key column in an \
                     extreme, got {}",
                    other.len()
                ),
            };
        };
        let value = ScalarValue::try_from(value).map_err(|e| {
            internal_datafusion_err!("SortKeySketchState: undecodable extreme: {e:?}")
        })?;
        match codec.encode(value.to_array()?.as_ref())?.as_slice() {
            [key] => Ok(Some(*key)),
            // A NULL extreme would encode to nothing. The extremes are the
            // sketch's *value* bounds, so a NULL there is a producer bug.
            _ => internal_err!("SortKeySketchState: extreme encoded to no key"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Float64Array, Int64Array, TimestampNanosecondArray, UInt64Array,
    };
    use std::sync::Arc;

    /// Encoding must be monotone: sorting the keys must give the same
    /// permutation as sorting the values. Checked over a set chosen to
    /// straddle every boundary the encoding has to get right — sign
    /// changes, zero, and the extremes of the type.
    #[test]
    fn ascending_keys_follow_value_order() {
        let values = vec![
            i64::MIN,
            i64::MIN + 1,
            -1_000_000,
            -1,
            0,
            1,
            1_000_000,
            i64::MAX - 1,
            i64::MAX,
        ];
        let codec =
            SortKeyCodec::try_new(&DataType::Int64, sort_options(false, false)).unwrap();
        let keys = codec.encode(&Int64Array::from(values.clone())).unwrap();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "keys must be strictly increasing for strictly increasing values: {keys:?}"
        );
    }

    /// `DESC` must invert that order while staying a bijection.
    #[test]
    fn descending_keys_reverse_value_order() {
        let values = vec![-5i64, 0, 5, 100];
        let codec =
            SortKeyCodec::try_new(&DataType::Int64, sort_options(true, false)).unwrap();
        let keys = codec.encode(&Int64Array::from(values.clone())).unwrap();
        assert!(
            keys.windows(2).all(|w| w[0] > w[1]),
            "DESC keys must strictly decrease for increasing values: {keys:?}"
        );
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(
                codec.decode(*key).unwrap(),
                ScalarValue::Int64(Some(*value))
            );
        }
    }

    /// Floats order by `total_cmp`, which puts `-0.0` below `+0.0` and
    /// sorts NaN to the ends by sign. Infinities are ordinary values.
    #[test]
    fn float_keys_follow_total_order() {
        let values = vec![
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.5,
            f64::INFINITY,
        ];
        let codec = SortKeyCodec::try_new(&DataType::Float64, sort_options(false, false))
            .unwrap();
        let keys = codec.encode(&Float64Array::from(values.clone())).unwrap();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "float keys must respect total_cmp order: {keys:?}"
        );
        // Round-trip preserves the sign of zero, which `==` on f64 would
        // not distinguish.
        assert_eq!(
            codec.decode(keys[2]).unwrap(),
            ScalarValue::Float64(Some(-0.0))
        );
        assert!(
            matches!(codec.decode(keys[2]).unwrap(), ScalarValue::Float64(Some(v)) if v.is_sign_negative())
        );
    }

    /// NaN and the infinities have defined places in `total_cmp`: a NaN
    /// sits beyond the infinity of its own sign, and the infinities are
    /// otherwise ordinary values. Arrow sorts floats by exactly this
    /// predicate (`ArrowNativeTypeOp::compare` delegates to `total_cmp`),
    /// so the key has to agree — otherwise the sketch would order its
    /// input differently from the `SortExec` that produced it.
    ///
    /// Unlike T-Digest, which interpolates centroid means and can turn
    /// `Inf - Inf` into NaN, the sketch only ever compares keys, so an
    /// infinity is exactly as safe here as any other value.
    #[test]
    fn nan_and_infinities_take_their_total_cmp_positions() {
        let values = vec![
            -f64::NAN,
            f64::NEG_INFINITY,
            -1.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NAN,
        ];
        assert!(
            values.windows(2).all(|w| w[0].total_cmp(&w[1]).is_lt()),
            "test premise: the fixture is in total_cmp order"
        );

        let codec = SortKeyCodec::try_new(&DataType::Float64, sort_options(false, false))
            .unwrap();
        let keys = codec.encode(&Float64Array::from(values.clone())).unwrap();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "keys must reproduce total_cmp order: {keys:?}"
        );

        for (key, value) in keys.iter().zip(&values) {
            match codec.decode(*key).unwrap() {
                ScalarValue::Float64(Some(decoded)) => assert_eq!(
                    decoded.total_cmp(value),
                    std::cmp::Ordering::Equal,
                    "decode must round-trip {value} bit-exactly, including NaN sign"
                ),
                other => panic!("expected Float64, got {other:?}"),
            }
        }
    }

    /// The two encoders have to agree. A column's routing must not depend
    /// on whether it happened to qualify for the integer fast path or fell
    /// through to arrow-row, so the orders they induce must be identical —
    /// including on the float edge cases.
    #[test]
    fn integer_keys_order_identically_to_arrow_row() {
        use datafusion::arrow::row::{RowConverter, SortField};

        let values = vec![
            -f64::NAN,
            f64::NEG_INFINITY,
            -7.25,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            7.25,
            f64::INFINITY,
            f64::NAN,
        ];
        let array = Float64Array::from(values.clone());

        let codec = SortKeyCodec::try_new(&DataType::Float64, sort_options(false, false))
            .unwrap();
        let keys = codec.encode(&array).unwrap();

        let converter =
            RowConverter::new(vec![SortField::new(DataType::Float64)]).unwrap();
        let rows = converter
            .convert_columns(&[Arc::new(array) as Arc<dyn Array>])
            .unwrap();

        let by_key = argsort(&keys);
        let by_row: Vec<usize> = {
            let mut order: Vec<usize> = (0..values.len()).collect();
            order.sort_by_key(|&i| rows.row(i).as_ref().to_vec());
            order
        };
        assert_eq!(
            by_key, by_row,
            "integer keys and arrow-row bytes must induce the same permutation"
        );
    }

    /// `SortOptions` without the struct-literal noise at every call site.
    fn sort_options(descending: bool, nulls_first: bool) -> SortOptions {
        SortOptions {
            descending,
            nulls_first,
        }
    }

    /// Ingest keys through a sketch, one `absorb_slice` per batch.
    fn sketch_batches(codec: &SortKeyCodec, batches: Vec<Vec<f64>>) -> KllSketch<u64> {
        let mut sketch = KllSketch::<u64>::new(64);
        for batch in batches {
            let keys = codec.encode(&Float64Array::from(batch)).unwrap();
            sketch.absorb_slice(&keys);
        }
        sketch
    }

    /// The extremes a sketch reports are what decides which shuffle files
    /// overlap which output partition, so losing one loses rows. They have
    /// to survive ingest even when a batch *begins and ends* with NaN,
    /// which is what a `total_cmp` sort does to any batch containing one.
    ///
    /// This is the case DataFusion's T-Digest gets wrong. It reads a
    /// batch's extremes from `first()` and `last()` alone and folds them in
    /// with `f64::min`/`f64::max`, which discard NaN rather than order it.
    /// Fed the same two batches, it reports the stream's range as
    /// `[1.0, 5.0]` and loses both infinities. Comparing keys with `Ord`
    /// over every element has no such blind spot.
    #[test]
    fn extremes_survive_a_batch_whose_ends_are_nan() {
        let codec = SortKeyCodec::try_new(&DataType::Float64, sort_options(false, false))
            .unwrap();
        let sketch = sketch_batches(
            &codec,
            vec![
                vec![1.0, 5.0],
                vec![f64::NAN, -f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
            ],
        );

        // Per total_cmp, -NaN is below every value and +NaN above every
        // value, so those are this stream's true extremes.
        let min = codec.decode(*sketch.min().unwrap()).unwrap();
        let max = codec.decode(*sketch.max().unwrap()).unwrap();
        assert!(
            matches!(min, ScalarValue::Float64(Some(v)) if v.is_nan() && v.is_sign_negative()),
            "min must be -NaN, got {min:?}"
        );
        assert!(
            matches!(max, ScalarValue::Float64(Some(v)) if v.is_nan() && v.is_sign_positive()),
            "max must be +NaN, got {max:?}"
        );
    }

    /// Same shape without NaN, which is the plainer data-loss case: a
    /// stream spanning both infinities must report spanning both
    /// infinities. T-Digest reports `[1.0, 5.0]` here once a NaN has
    /// appeared in the same batch as the infinities.
    #[test]
    fn infinities_are_reported_as_the_extremes_they_are() {
        let codec = SortKeyCodec::try_new(&DataType::Float64, sort_options(false, false))
            .unwrap();
        let sketch = sketch_batches(
            &codec,
            vec![vec![1.0, 5.0], vec![f64::INFINITY, f64::NEG_INFINITY]],
        );
        assert_eq!(
            codec.decode(*sketch.min().unwrap()).unwrap(),
            ScalarValue::Float64(Some(f64::NEG_INFINITY))
        );
        assert_eq!(
            codec.decode(*sketch.max().unwrap()).unwrap(),
            ScalarValue::Float64(Some(f64::INFINITY))
        );
    }

    /// Batch arrival order must not change the answer. T-Digest's is order
    /// dependent: the first batch assigns extremes directly, later batches
    /// fold theirs in with NaN-dropping comparisons, so the same data gives
    /// different bounds depending on which batch landed first.
    #[test]
    fn extremes_do_not_depend_on_batch_order() {
        let codec = SortKeyCodec::try_new(&DataType::Float64, sort_options(false, false))
            .unwrap();
        let nan_batch = vec![f64::NAN, -f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        let plain_batch = vec![1.0, 5.0];

        let nan_first =
            sketch_batches(&codec, vec![nan_batch.clone(), plain_batch.clone()]);
        let nan_second = sketch_batches(&codec, vec![plain_batch, nan_batch]);

        assert_eq!(nan_first.min(), nan_second.min(), "min is order dependent");
        assert_eq!(nan_first.max(), nan_second.max(), "max is order dependent");
    }

    /// Build a sketch over `values`, ingested as one batch.
    fn int_sketch(values: Vec<Option<i64>>, options: SortOptions) -> SortKeySketch {
        let codec = SortKeyCodec::try_new(&DataType::Int64, options).unwrap();
        let mut sketch = SortKeySketch::new(codec);
        sketch.ingest(&Int64Array::from(values)).unwrap();
        sketch
    }

    /// NULLs are counted, not sketched, so they must show up in the total
    /// without disturbing the values.
    #[test]
    fn nulls_are_counted_beside_the_values() {
        let sketch = int_sketch(
            vec![Some(10), None, Some(20), None, None],
            sort_options(false, true),
        );
        assert_eq!(sketch.count(), 5, "population is values plus NULLs");
        assert_eq!(sketch.null_count(), 3);
    }

    /// With no NULLs the population rank is the value rank, so the remap
    /// must be a no-op. This is the case the general path's `rank <= n`
    /// test would get wrong at q = 0, which is why it has its own branch.
    #[test]
    fn quantiles_are_unshifted_when_nothing_is_null() {
        let values: Vec<Option<i64>> = (1..=100).map(Some).collect();
        let sketch = int_sketch(values, sort_options(false, true));
        assert_eq!(
            sketch.quantile(0.0).unwrap(),
            Some(ScalarValue::Int64(Some(1))),
            "q=0 must be the smallest value, not a NULL"
        );
        assert_eq!(
            sketch.quantile(1.0).unwrap(),
            Some(ScalarValue::Int64(Some(100)))
        );
    }

    /// Half NULL, NULLs first. The bottom half of the population is the
    /// NULL run, so every quantile below the midpoint is a NULL and the
    /// top half compresses the whole value range into `q > 0.5`. A
    /// consumer that skipped the remap would report the value median at
    /// q=0.5 and skew every cut.
    #[test]
    fn nulls_first_shifts_the_value_range_upward() {
        let mut values: Vec<Option<i64>> = (1..=50).map(Some).collect();
        values.extend(std::iter::repeat_n(None, 50));
        let sketch = int_sketch(values, sort_options(false, true));
        assert_eq!(sketch.count(), 100);
        assert_eq!(sketch.null_count(), 50);

        let null = ScalarValue::Int64(None);
        assert_eq!(sketch.quantile(0.0).unwrap(), Some(null.clone()));
        assert_eq!(sketch.quantile(0.25).unwrap(), Some(null.clone()));
        assert_eq!(
            sketch.quantile(0.5).unwrap(),
            Some(null),
            "the NULL run reaches exactly the midpoint"
        );
        // Past the run, rank 0.75·100 = 75 is value 25 of 50.
        assert_eq!(
            sketch.quantile(0.75).unwrap(),
            Some(ScalarValue::Int64(Some(25)))
        );
        assert_eq!(
            sketch.quantile(1.0).unwrap(),
            Some(ScalarValue::Int64(Some(50)))
        );
    }

    /// Same data, NULLs last: the values now occupy the bottom half and
    /// the NULL run the top.
    #[test]
    fn nulls_last_leaves_the_value_range_at_the_bottom() {
        let mut values: Vec<Option<i64>> = (1..=50).map(Some).collect();
        values.extend(std::iter::repeat_n(None, 50));
        let sketch = int_sketch(values, sort_options(false, false));

        assert_eq!(
            sketch.quantile(0.25).unwrap(),
            Some(ScalarValue::Int64(Some(25))),
            "rank 25 of 100 is value 25 of 50"
        );
        assert_eq!(
            sketch.quantile(0.5).unwrap(),
            Some(ScalarValue::Int64(Some(50))),
            "the value run ends exactly at the midpoint"
        );
        assert_eq!(
            sketch.quantile(0.75).unwrap(),
            Some(ScalarValue::Int64(None))
        );
        assert_eq!(
            sketch.quantile(1.0).unwrap(),
            Some(ScalarValue::Int64(None))
        );
    }

    /// All NULL: every quantile is a NULL, and nothing consults the empty
    /// value sketch.
    #[test]
    fn an_all_null_column_answers_null_everywhere() {
        let sketch = int_sketch(vec![None; 8], sort_options(false, true));
        for q in [0.0, 0.5, 1.0] {
            assert_eq!(
                sketch.quantile(q).unwrap(),
                Some(ScalarValue::Int64(None)),
                "q={q}"
            );
        }
        assert_eq!(sketch.count(), 8);
    }

    /// With no values at all the NULL run is unbounded on both sides, so
    /// both extremes are NULL whichever end the run nominally occupies.
    ///
    /// The end it does not occupy has no value to fall back to, and
    /// answering `None` there would claim nothing was observed while
    /// `count` says otherwise. `cut_partitions` routes files on the pair,
    /// so half a range routes half the rows.
    #[test]
    fn an_all_null_column_is_null_at_both_extremes() {
        for nulls_first in [true, false] {
            let sketch = int_sketch(vec![None; 8], sort_options(false, nulls_first));
            assert_eq!(
                sketch.min().unwrap(),
                Some(ScalarValue::Int64(None)),
                "nulls_first={nulls_first}"
            );
            assert_eq!(
                sketch.max().unwrap(),
                Some(ScalarValue::Int64(None)),
                "nulls_first={nulls_first}"
            );
        }
    }

    /// Nothing observed at all is distinct from observing NULLs.
    #[test]
    fn an_empty_sketch_answers_none_not_null() {
        let codec =
            SortKeyCodec::try_new(&DataType::Int64, sort_options(false, true)).unwrap();
        let sketch = SortKeySketch::new(codec);
        assert_eq!(sketch.count(), 0);
        assert_eq!(sketch.quantile(0.5).unwrap(), None);
        assert_eq!(sketch.min().unwrap(), None);
        assert!(sketch.cuts(4).unwrap().is_empty());
    }

    /// The extremes follow NULL placement: with NULLs first the least row
    /// is a NULL, and the greatest is still the largest value.
    #[test]
    fn extremes_account_for_where_nulls_sort() {
        let values = vec![Some(10), None, Some(20)];
        let first = int_sketch(values.clone(), sort_options(false, true));
        assert_eq!(first.min().unwrap(), Some(ScalarValue::Int64(None)));
        assert_eq!(first.max().unwrap(), Some(ScalarValue::Int64(Some(20))));

        let last = int_sketch(values, sort_options(false, false));
        assert_eq!(last.min().unwrap(), Some(ScalarValue::Int64(Some(10))));
        assert_eq!(last.max().unwrap(), Some(ScalarValue::Int64(None)));
    }

    /// Merging folds both the value sketches and the NULL counts. Losing
    /// the second sketch's NULLs would silently shift every quantile.
    #[test]
    fn merge_folds_values_and_null_counts() {
        let options = sort_options(false, true);
        let mut left = int_sketch(vec![Some(1), Some(2), None], options);
        let right = int_sketch(vec![Some(3), Some(4), None, None], options);
        left.merge(right).unwrap();

        assert_eq!(left.count(), 7, "3 + 4 rows");
        assert_eq!(left.null_count(), 3, "1 + 2 NULLs");
        assert_eq!(left.max().unwrap(), Some(ScalarValue::Int64(Some(4))));
        assert_eq!(left.min().unwrap(), Some(ScalarValue::Int64(None)));
    }

    /// Sketches of different keys must not fold together. The result would
    /// look like a perfectly ordinary distribution of nothing in
    /// particular.
    #[test]
    fn merge_rejects_a_different_sort_key() {
        let options = sort_options(false, true);
        let mut ints = int_sketch(vec![Some(1)], options);

        let float_codec = SortKeyCodec::try_new(&DataType::Float64, options).unwrap();
        let floats = SortKeySketch::new(float_codec);
        let err = ints
            .merge(floats)
            .expect_err("merging Int64 with Float64 must be refused");
        assert!(
            err.to_string().contains("different sort keys"),
            "got: {err}"
        );

        // Direction is part of the key's identity too: the same column
        // sketched ASC and DESC has keys running opposite ways.
        let mut ascending = int_sketch(vec![Some(1)], sort_options(false, true));
        let descending = int_sketch(vec![Some(1)], sort_options(true, true));
        assert!(
            ascending.merge(descending).is_err(),
            "ASC and DESC keys are inverses; folding them is meaningless"
        );
    }

    /// Boundaries as the `Int64` scalars `cuts` returns for an `Int64` key.
    fn i64_cuts(values: &[i64]) -> Vec<ScalarValue> {
        values
            .iter()
            .map(|v| ScalarValue::Int64(Some(*v)))
            .collect()
    }

    /// A population of 12 over K=4 gives each partition a share of 3, which is
    /// the smallest size where the end effects of the half-open convention stay
    /// within one row. At 10 the same cuts spread by 2 and these assertions
    /// would be measuring integer rounding rather than the repair.
    ///
    /// No NULLs, so nothing displaces anything and the ranks land where the
    /// population quartiles say: 3, 6, 9.
    #[test]
    fn cuts_balance_when_nothing_is_null() {
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let column = vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
        ];
        let sketch = int_sketch(column, sort_options(false, true));

        let cuts = sketch.cuts(4).unwrap();

        assert_eq!(cuts, i64_cuts(&[3, 6, 9]));
        assert_eq!(partition_sizes(0, &values, &cuts, true), vec![2, 3, 3, 4]);
    }

    /// A run of 2 against a share of 3 fits inside partition 0 beside the
    /// values below the first cut, so every boundary keeps its population rank
    /// shifted down by the run and nothing needs repairing.
    #[test]
    fn cuts_balance_when_the_run_fits_inside_one_partition() {
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let column = vec![
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
        ];
        let sketch = int_sketch(column, sort_options(false, true));

        let cuts = sketch.cuts(4).unwrap();

        assert_eq!(cuts, i64_cuts(&[1, 4, 7]));
        assert_eq!(partition_sizes(2, &values, &cuts, true), vec![2, 3, 3, 4]);
    }

    /// A run of 6 against a share of 3 is indivisible, so partition 0 takes it
    /// whole and its size cannot be improved. What the boundaries above it owe
    /// is an even split of the six values — `[2, 2, 2]`, not the `[4, 1, 1]`
    /// that keeping the raw population ranks would give.
    #[test]
    fn cuts_balance_when_the_run_outgrows_one_partition() {
        let values = vec![1, 2, 3, 4, 5, 6];
        let column = vec![
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
        ];
        let sketch = int_sketch(column, sort_options(false, true));

        let cuts = sketch.cuts(4).unwrap();

        assert_eq!(cuts, i64_cuts(&[1, 3, 5]));
        assert_eq!(partition_sizes(6, &values, &cuts, true), vec![6, 2, 2, 2]);
    }

    /// The run leaves fewer values than partitions to spread them over, so the
    /// best the boundaries can do is one value each. Every cut is still a real
    /// value: a NULL boundary would silently drop its partition.
    #[test]
    fn cuts_balance_when_the_run_leaves_one_value_per_partition() {
        let values = vec![1, 2, 3];
        let column = vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
        ];
        let sketch = int_sketch(column, sort_options(false, true));

        let cuts = sketch.cuts(4).unwrap();

        assert_eq!(cuts, i64_cuts(&[1, 2, 3]));
        assert_eq!(partition_sizes(9, &values, &cuts, true), vec![9, 1, 1, 1]);
    }

    /// K=8 over 6 values: there are not enough distinct values to give every
    /// partition one, so the top boundary repeats and the partition between the
    /// repeated pair gets nothing. Repeating beats emitting a NULL or a
    /// decreasing boundary, both of which consumers read as a dropped range.
    #[test]
    fn cuts_balance_when_partitions_outnumber_the_values() {
        let values = vec![1, 2, 3, 4, 5, 6];
        let column = vec![
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
        ];
        let sketch = int_sketch(column, sort_options(false, true));

        let cuts = sketch.cuts(8).unwrap();

        assert_eq!(cuts, i64_cuts(&[1, 2, 3, 4, 5, 6, 6]));
        assert_eq!(
            partition_sizes(6, &values, &cuts, true),
            vec![6, 1, 1, 1, 1, 1, 0, 1]
        );
    }

    /// Partition sizes a router would produce from `cuts`, under the half-open
    /// convention every consumer uses: partition 0 takes the NULLs and every
    /// value below `cuts[0]`, partition `i` takes `[cuts[i - 1], cuts[i])`, and
    /// the last takes everything from the final cut up.
    fn partition_sizes(
        nulls: usize,
        values: &[i64],
        cuts: &[ScalarValue],
        nulls_first: bool,
    ) -> Vec<usize> {
        let bound = |cut: &ScalarValue| match cut {
            ScalarValue::Int64(Some(value)) => *value,
            other => panic!("expected a non-NULL Int64 boundary, got {other:?}"),
        };
        let count = |predicate: &dyn Fn(i64) -> bool| {
            values.iter().filter(|value| predicate(**value)).count()
        };
        let first = bound(&cuts[0]);
        let last = bound(cuts.last().expect("at least one cut"));
        // The run rides the partition at its own end of the sort order.
        let (low_nulls, high_nulls) = if nulls_first { (nulls, 0) } else { (0, nulls) };
        let mut sizes = vec![low_nulls + count(&|value| value < first)];
        for pair in cuts.windows(2) {
            let (lo, hi) = (bound(&pair[0]), bound(&pair[1]));
            sizes.push(count(&|value| lo <= value && value < hi));
        }
        sizes.push(high_nulls + count(&|value| value >= last));
        sizes
    }

    /// A run shorter than one partition's share costs no balance at all: the
    /// boundaries keep their population ranks, so the run simply shares the
    /// lowest partition with the values below the first cut.
    #[test]
    fn cuts_are_unrepaired_when_the_null_run_is_small() {
        let mut values: Vec<Option<i64>> = vec![None; 10];
        values.extend((1..=90).map(Some));
        let sketch = int_sketch(values, sort_options(false, true));

        let cuts = sketch.cuts(4).unwrap();
        // Population ranks 25/50/75 minus the 10 NULLs, with no clamping.
        assert_eq!(
            cuts,
            vec![
                ScalarValue::Int64(Some(15)),
                ScalarValue::Int64(Some(40)),
                ScalarValue::Int64(Some(65)),
            ]
        );
        // Which leaves 10 NULLs + values 1..14, then 25, 25 and 26 rows: the
        // NULL run cost one row of balance against a perfect 25 apiece.
    }

    /// The `nulls_last` mirror. The run sits above the values, so every
    /// adjustment reverses: boundaries pull down toward the maximum and the run
    /// takes the *top* partition. Same properties as its `nulls_first` twin,
    /// and the shared no-NULL cases must agree between the two.
    #[test]
    fn cuts_mirror_the_repair_when_nulls_sort_last() {
        for (nulls, value_count, partitions) in [
            (60usize, 40i64, 4usize),
            (90, 10, 4),
            (60, 40, 8),
            (10, 90, 4),
            (95, 5, 4),
        ] {
            let values: Vec<i64> = (1..=value_count).collect();
            let mut column: Vec<Option<i64>> = values.iter().copied().map(Some).collect();
            column.extend(std::iter::repeat_n(None, nulls));
            let sketch = int_sketch(column, sort_options(false, false));

            let cuts = sketch.cuts(partitions).unwrap();
            let context =
                format!("{nulls} NULLs last, {value_count} values, K={partitions}");
            assert_eq!(cuts.len(), partitions - 1, "{context}");
            assert!(
                cuts.iter().all(|cut| !cut.is_null()),
                "{context}: got {cuts:?}"
            );
            assert!(
                cuts.windows(2).all(|pair| pair[0] <= pair[1]),
                "{context}: boundaries must be non-decreasing, got {cuts:?}"
            );

            let sizes = partition_sizes(nulls, &values, &cuts, false);
            assert_eq!(
                sizes.iter().sum::<usize>(),
                nulls + value_count as usize,
                "{context}: every row lands somewhere, got {sizes:?}"
            );
            // The run takes the last partition, so only the others can balance.
            let below_the_run = &sizes[..sizes.len() - 1];
            let spread =
                below_the_run.iter().max().unwrap() - below_the_run.iter().min().unwrap();
            assert!(
                spread <= 1,
                "{context}: partitions below the run should differ by at most \
                 one row, got {sizes:?}"
            );
        }
    }

    /// The population rank sits *below* the even split once the run owns the
    /// top partition, so honouring it strands the partitions beneath: cuts
    /// `[1, 3]` give sizes `[0, 2, 3]`, an empty partition beside a double one.
    /// The even split is what the run leaves room for, and nothing above it can
    /// be improved by consulting a rank the run already displaced.
    #[test]
    fn cuts_fill_the_low_partitions_when_the_run_owns_the_top_one() {
        let values = vec![1, 2, 3];
        let column = vec![Some(1), Some(2), Some(3), None, None];
        let sketch = int_sketch(column, sort_options(false, false));

        let cuts = sketch.cuts(3).unwrap();

        assert_eq!(cuts, i64_cuts(&[2, 3]));
        assert_eq!(partition_sizes(2, &values, &cuts, false), vec![1, 1, 3]);
    }

    /// With no NULLs there is no run at either end, so the two layouts have
    /// nothing to mirror and must produce identical boundaries.
    #[test]
    fn cuts_agree_across_null_placement_when_nothing_is_null() {
        let column: Vec<Option<i64>> = (1..=100).map(Some).collect();
        let first = int_sketch(column.clone(), sort_options(false, true));
        let last = int_sketch(column, sort_options(false, false));
        for partitions in [2usize, 4, 8, 16] {
            assert_eq!(
                first.cuts(partitions).unwrap(),
                last.cuts(partitions).unwrap(),
                "K={partitions}"
            );
        }
    }

    /// With no NULLs there is no run to sit above or below the values, so
    /// `nulls_last` needs nothing mirrored and must not be refused.
    #[test]
    fn cuts_allow_nulls_last_without_nulls() {
        let sketch =
            int_sketch((1..=100).map(Some).collect(), sort_options(false, false));
        let cuts = sketch.cuts(4).unwrap();
        assert_eq!(
            cuts,
            vec![
                ScalarValue::Int64(Some(25)),
                ScalarValue::Int64(Some(50)),
                ScalarValue::Int64(Some(75)),
            ]
        );
    }

    /// A NULL run ending exactly on a cut still belongs to the NULL side:
    /// the rank it occupies is the run's last row, not the values' first.
    /// 75 NULLs of 100 rows puts the 0.75 cut precisely there.
    #[test]
    fn a_cut_landing_on_the_end_of_the_null_run_is_null() {
        let mut values: Vec<Option<i64>> = vec![None; 75];
        values.extend((1..=25).map(Some));
        let sketch = int_sketch(values, sort_options(false, true));
        assert_eq!(
            sketch.quantile(0.75).unwrap(),
            Some(ScalarValue::Int64(None)),
            "rank 75 of 100 is the last NULL, so three of four partitions \
             hold only NULLs"
        );
    }

    /// The design priced and dropped in `benchmarks/benches/
    /// quantile_sketch.rs` as `kll_norm_u128`, kept here as an oracle.
    ///
    /// It puts NULLs *inside* the key, tagged above the value bits, so the
    /// sketch sees the whole population in order and a quantile is a
    /// straight lookup with no arithmetic. That makes it the independent
    /// check on [`SortKeySketch::quantile`], whose whole job is to
    /// reproduce this answer while keeping the key 8 bytes wide.
    ///
    /// Value encoding is shared, so a disagreement can only come from NULL
    /// placement or the rank remap.
    struct NullsInKeyOracle {
        sketch: KllSketch<u128>,
        codec: SortKeyCodec,
        null_tag: u128,
    }

    impl NullsInKeyOracle {
        fn new(values: &[Option<i64>], options: SortOptions) -> Self {
            let codec = SortKeyCodec::try_new(&DataType::Int64, options).unwrap();
            // The tag alone decides which side the NULL run sits on.
            let (null_tag, value_tag) = if options.nulls_first {
                (0u128, 1u128)
            } else {
                (1u128, 0u128)
            };
            let keys: Vec<u128> = values
                .iter()
                .map(|value| match value {
                    None => null_tag << 64,
                    Some(value) => {
                        let key =
                            codec.encode(&Int64Array::from(vec![*value])).unwrap()[0];
                        (value_tag << 64) | key as u128
                    }
                })
                .collect();
            let mut sketch = KllSketch::<u128>::new(KLL_K);
            sketch.absorb_slice(&keys);
            Self {
                sketch,
                codec,
                null_tag,
            }
        }

        fn quantile(&self, q: f64) -> Option<ScalarValue> {
            let key = self.sketch.quantile(q)?;
            if (key >> 64) == self.null_tag {
                Some(self.codec.null_value().unwrap())
            } else {
                Some(self.codec.decode(*key as u64).unwrap())
            }
        }
    }

    /// Differential test: the shipped out-of-band-NULL sketch must answer
    /// exactly what the nulls-in-the-key oracle answers, across every NULL
    /// fraction and both placements.
    ///
    /// Both sketches are exact here — the fixtures are far below `KLL_K`,
    /// so nothing compacts — which means any disagreement is the rank
    /// remap and not sketch error.
    #[test]
    fn quantiles_match_the_nulls_in_key_oracle() {
        // Deliberately includes 0 and every-row-NULL, plus fractions whose
        // ranks land exactly on the run boundary for some q.
        let null_counts = [0usize, 1, 25, 50, 60, 75, 99, 100];
        let quantile_probes: Vec<f64> = (0..=20).map(|step| step as f64 / 20.0).collect();

        for nulls in null_counts {
            let mut values: Vec<Option<i64>> = vec![None; nulls];
            values.extend((1..=(100 - nulls) as i64).map(Some));
            for nulls_first in [true, false] {
                let options = sort_options(false, nulls_first);
                let oracle = NullsInKeyOracle::new(&values, options);
                let sketch = int_sketch(values.clone(), options);

                assert_eq!(
                    sketch.count(),
                    100,
                    "nulls={nulls} nulls_first={nulls_first}: population size"
                );
                for &q in &quantile_probes {
                    assert_eq!(
                        sketch.quantile(q).unwrap(),
                        oracle.quantile(q),
                        "nulls={nulls} nulls_first={nulls_first} q={q}: \
                         out-of-band NULLs disagreed with nulls-in-key"
                    );
                }
            }
        }
    }

    /// The same agreement has to hold under `DESC`, where the value keys
    /// are inverted but the NULL run still sits where `nulls_first` says.
    #[test]
    fn descending_quantiles_match_the_nulls_in_key_oracle() {
        let mut values: Vec<Option<i64>> = vec![None; 30];
        values.extend((1..=70).map(Some));
        for nulls_first in [true, false] {
            let options = sort_options(true, nulls_first);
            let oracle = NullsInKeyOracle::new(&values, options);
            let sketch = int_sketch(values.clone(), options);
            for step in 0..=20 {
                let q = step as f64 / 20.0;
                assert_eq!(
                    sketch.quantile(q).unwrap(),
                    oracle.quantile(q),
                    "DESC nulls_first={nulls_first} q={q}"
                );
            }
        }
    }

    /// Indices of `keys` in ascending key order.
    fn argsort(keys: &[u64]) -> Vec<usize> {
        let mut order: Vec<usize> = (0..keys.len()).collect();
        order.sort_by_key(|&i| keys[i]);
        order
    }

    /// The worked table in `impl_sortable_float`'s docs, asserted. Hex
    /// written by hand into a comment rots silently; pinning it here makes
    /// a drift fail instead.
    #[test]
    fn float_key_table_in_docs_is_accurate() {
        let table: [(f64, u64); 10] = [
            (-f64::NAN, 0x0007FFFFFFFFFFFF),
            (f64::NEG_INFINITY, 0x000FFFFFFFFFFFFF),
            (-2.0, 0x3FFFFFFFFFFFFFFF),
            (-1.0, 0x400FFFFFFFFFFFFF),
            (-0.0, 0x7FFFFFFFFFFFFFFF),
            (0.0, 0x8000000000000000),
            (1.0, 0xBFF0000000000000),
            (2.0, 0xC000000000000000),
            (f64::INFINITY, 0xFFF0000000000000),
            (f64::NAN, 0xFFF8000000000000),
        ];
        for (value, expected) in table {
            assert_eq!(
                value.to_key(),
                expected,
                "{value} should key to {expected:#018X}, got {:#018X}",
                value.to_key()
            );
        }
        assert!(
            table.windows(2).all(|w| w[0].1 < w[1].1),
            "the table is written in ascending value order, so its keys \
             must ascend too"
        );
    }

    /// Unsigned keys must not be shifted the way signed ones are, or the
    /// top half of the range would wrap below the bottom.
    #[test]
    fn unsigned_keys_span_full_range_in_order() {
        let values = vec![0u64, 1, 1 << 62, u64::MAX - 1, u64::MAX];
        let codec =
            SortKeyCodec::try_new(&DataType::UInt64, sort_options(false, false)).unwrap();
        let keys = codec.encode(&UInt64Array::from(values.clone())).unwrap();
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "{keys:?}");
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(
                codec.decode(*key).unwrap(),
                ScalarValue::UInt64(Some(*value))
            );
        }
    }

    /// The motivating case: nanosecond timestamps round-trip exactly at
    /// magnitudes where an `f64` cast would quantize them. The timezone
    /// rides on the `DataType` rather than the key, so it has to survive
    /// the decode as well.
    #[test]
    fn nanosecond_timestamps_round_trip_exactly_with_timezone() {
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        // 2026-08-12T00:00:00Z in nanos, then the next three nanoseconds.
        // Above 2^53, so an f64 cast would collapse them onto one value.
        let base = 1_786_233_600_000_000_000i64;
        let values = vec![base, base + 1, base + 2, base + 3];
        assert!(
            base as f64 as i64 != base + 1,
            "test premise: f64 must be unable to separate adjacent nanos here"
        );

        let array = TimestampNanosecondArray::from(values.clone())
            .with_timezone("UTC".to_string());
        let codec =
            SortKeyCodec::try_new(&data_type, sort_options(false, false)).unwrap();
        let keys = codec.encode(&array).unwrap();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "adjacent nanoseconds must stay distinct and ordered: {keys:?}"
        );
        for (key, value) in keys.iter().zip(&values) {
            assert_eq!(
                codec.decode(*key).unwrap(),
                ScalarValue::TimestampNanosecond(Some(*value), Some("UTC".into())),
                "decode must preserve both the instant and the timezone"
            );
        }
    }

    /// Encode `ascending` under both directions, asserting the keys run
    /// the way the direction asks and that each one decodes back to the
    /// value it came from.
    ///
    /// `ascending` must be in `total_cmp` order and free of duplicates,
    /// since the keys are checked for strict monotonicity.
    fn check_dispatched<T: ArrowPrimitiveType>(ascending: Vec<T::Native>) {
        let array = PrimitiveArray::<T>::from_iter_values(ascending.iter().copied());
        let data_type = array.data_type().clone();
        for descending in [false, true] {
            let codec = SortKeyCodec::try_new(&data_type, sort_options(descending, true))
                .unwrap_or_else(|| {
                    panic!("{data_type:?} is dispatched but try_new declined it")
                });
            let keys = codec.encode(&array).unwrap();
            assert_eq!(keys.len(), ascending.len(), "{data_type:?}");

            for pair in keys.windows(2) {
                let [lo, hi] = pair else {
                    unreachable!("windows(2) yields pairs")
                };
                let ordered = if descending { lo > hi } else { lo < hi };
                assert!(
                    ordered,
                    "{data_type:?} descending={descending}: {lo:#018x} then {hi:#018x}"
                );
            }

            for (key, value) in keys.iter().zip(&ascending) {
                let expected =
                    ScalarValue::new_primitive::<T>(Some(*value), &data_type).unwrap();
                assert_eq!(
                    codec.decode(*key).unwrap(),
                    expected,
                    "{data_type:?} descending={descending}"
                );
            }
        }
    }

    /// Every type `dispatch_sortable!` admits, end to end: the allowlist
    /// accepts it, the keys run the way the direction asks, and the decode
    /// arm hands back the value that went in.
    ///
    /// The narrow widths are their own code rather than a special case of
    /// the 64-bit ones. `impl_sortable_float!(f32, u32, 32)` zero-extends a
    /// 32-bit key that DESC then inverts across all 64 bits, and the narrow
    /// signed arms sign-extend on the way out and truncate on the way back.
    #[test]
    fn every_dispatched_type_encodes_in_order_and_decodes_back() {
        check_dispatched::<Int8Type>(vec![i8::MIN, -1, 0, 1, i8::MAX]);
        check_dispatched::<Int16Type>(vec![i16::MIN, -1, 0, 1, i16::MAX]);
        check_dispatched::<Int32Type>(vec![i32::MIN, -1, 0, 1, i32::MAX]);
        check_dispatched::<Int64Type>(vec![i64::MIN, -1, 0, 1, i64::MAX]);

        check_dispatched::<UInt8Type>(vec![0, 1, u8::MAX]);
        check_dispatched::<UInt16Type>(vec![0, 1, u16::MAX]);
        check_dispatched::<UInt32Type>(vec![0, 1, u32::MAX]);
        check_dispatched::<UInt64Type>(vec![0, 1, u64::MAX]);

        // `total_cmp` order, so both NaNs sit outside their own infinity
        // and the two zeros are distinct.
        check_dispatched::<Float32Type>(vec![
            -f32::NAN,
            f32::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f32::INFINITY,
            f32::NAN,
        ]);
        check_dispatched::<Float64Type>(vec![
            -f64::NAN,
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NAN,
        ]);

        check_dispatched::<Date32Type>(vec![i32::MIN, -1, 0, 1, i32::MAX]);
        check_dispatched::<Date64Type>(vec![i64::MIN, -1, 0, 1, i64::MAX]);

        // Times are an offset within a day, so their range is bounded by
        // the unit rather than by the width that carries it.
        check_dispatched::<Time32SecondType>(vec![0, 1, 86_399]);
        check_dispatched::<Time32MillisecondType>(vec![0, 1, 86_399_999]);
        check_dispatched::<Time64MicrosecondType>(vec![0, 1, 86_399_999_999]);
        check_dispatched::<Time64NanosecondType>(vec![0, 1, 86_399_999_999_999]);

        check_dispatched::<TimestampSecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        check_dispatched::<TimestampMillisecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        check_dispatched::<TimestampMicrosecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        check_dispatched::<TimestampNanosecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);

        check_dispatched::<DurationSecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        check_dispatched::<DurationMillisecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        check_dispatched::<DurationMicrosecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        check_dispatched::<DurationNanosecondType>(vec![i64::MIN, -1, 0, 1, i64::MAX]);
    }

    /// NULLs leave the key stream entirely, so the caller can count them
    /// separately and place them by `nulls_first` at routing time.
    #[test]
    fn encode_skips_nulls_and_keeps_value_order() {
        let array = Int64Array::from(vec![Some(3), None, Some(1), None, Some(2)]);
        let codec =
            SortKeyCodec::try_new(&DataType::Int64, sort_options(false, false)).unwrap();
        let keys = codec.encode(&array).unwrap();
        assert_eq!(keys.len(), 3, "one key per non-NULL value");
        assert_eq!(array.null_count(), 2, "count stays available on the array");
        let decoded: Vec<ScalarValue> =
            keys.iter().map(|k| codec.decode(*k).unwrap()).collect();
        assert_eq!(
            decoded,
            vec![
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ],
            "non-NULL values keep their relative row order"
        );
    }

    /// Types outside the allowlist must decline rather than encode wrongly
    /// — that `None` is what routes them to the arrow-row path.
    #[test]
    fn unsupported_types_decline() {
        for data_type in [
            DataType::Utf8,
            DataType::Binary,
            DataType::Decimal128(38, 10),
            DataType::Boolean,
        ] {
            assert!(
                SortKeyCodec::try_new(&data_type, sort_options(false, false)).is_none(),
                "{data_type:?} must fall through to the arrow-row path"
            );
        }
    }

    /// A codec must reject an array of the wrong type instead of
    /// reinterpreting its bits.
    #[test]
    fn encode_rejects_mismatched_array_type() {
        let codec =
            SortKeyCodec::try_new(&DataType::Int64, sort_options(false, false)).unwrap();
        let array: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let err = codec
            .encode(array.as_ref())
            .expect_err("type mismatch must not silently reinterpret");
        assert!(err.to_string().contains("built for"), "got: {err}");
    }
    /// A round trip has to preserve every answer the sketch gives, not just
    /// its byte count: the extremes exactly, and every rank the compactor
    /// stack encodes. Rebuilding the stack wrongly — a dropped level, a
    /// weight off by a factor of two — leaves `count` intact while moving
    /// the quantiles, so the quantile sweep is the assertion that matters.
    #[test]
    fn wire_round_trip_preserves_every_answer() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let codec = SortKeyCodec::try_new(&DataType::Float64, options).unwrap();
        let mut original = SortKeySketch::new(codec);
        // Past k so the stack has several levels with real weights, rather
        // than one level where every item weighs 1 and a broken rebuild
        // would still answer correctly.
        let values: Vec<Option<f64>> = (0..5_000)
            .map(|row| Some(1.0 + row as f64 * 99.0 / 5_000.0))
            .chain((0..40).map(|_| None))
            .collect();
        original.ingest(&Float64Array::from(values)).unwrap();

        let decoded =
            SortKeySketch::try_from_proto(&original.to_proto().unwrap(), options)
                .unwrap();

        assert_eq!(decoded.count(), original.count());
        assert_eq!(decoded.null_count(), original.null_count());
        assert_eq!(decoded.codec(), original.codec());
        assert_eq!(decoded.min().unwrap(), original.min().unwrap());
        assert_eq!(decoded.max().unwrap(), original.max().unwrap());
        for step in 0..=100 {
            let q = step as f64 / 100.0;
            assert_eq!(
                decoded.quantile(q).unwrap(),
                original.quantile(q).unwrap(),
                "quantile({q}) diverged across the wire"
            );
        }
        assert_eq!(decoded.cuts(8).unwrap(), original.cuts(8).unwrap());
    }

    /// An empty sketch still has to survive the wire: a task may hold a
    /// partition slot it never executed, and the report emits every slot.
    #[test]
    fn wire_round_trip_preserves_empty_sketch() {
        let options = SortOptions::default();
        let codec = SortKeyCodec::try_new(&DataType::Int64, options).unwrap();
        let original = SortKeySketch::new(codec);

        let decoded =
            SortKeySketch::try_from_proto(&original.to_proto().unwrap(), options)
                .unwrap();

        assert_eq!(decoded.count(), 0);
        assert_eq!(decoded.null_count(), 0);
        assert_eq!(decoded.min().unwrap(), None);
        assert_eq!(decoded.max().unwrap(), None);
        assert!(decoded.cuts(8).unwrap().is_empty());
    }

    /// A payload carrying keys but no extremes is the one shape that makes
    /// `cuts` fallible, so it has to die at decode instead.
    #[test]
    fn wire_refuses_retained_keys_without_extremes() {
        let options = SortOptions::default();
        let codec = SortKeyCodec::try_new(&DataType::Int64, options).unwrap();
        let mut original = SortKeySketch::new(codec);
        original.ingest(&Int64Array::from(vec![1, 2, 3])).unwrap();

        let mut proto = original.to_proto().unwrap();
        proto.key_max.clear();

        let err = SortKeySketch::try_from_proto(&proto, options)
            .expect_err("retained keys without a key_max must be refused");
        assert!(
            err.to_string().contains("carries both extremes"),
            "got: {err}"
        );
    }

    /// NULLs are counted beside the sketch rather than in it, so they have
    /// their own way of not surviving serialization.
    #[test]
    fn wire_round_trip_preserves_a_nulls_only_sketch() {
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let codec = SortKeyCodec::try_new(&DataType::Int64, options).unwrap();
        let mut original = SortKeySketch::new(codec);
        original
            .ingest(&Int64Array::from(vec![None, None, None]))
            .unwrap();

        let decoded =
            SortKeySketch::try_from_proto(&original.to_proto().unwrap(), options)
                .unwrap();

        assert_eq!(decoded.count(), 3);
        assert_eq!(decoded.null_count(), 3);
        // NULLs sort last here, so both extremes are the typed NULL.
        assert_eq!(decoded.min().unwrap(), original.min().unwrap());
        assert_eq!(decoded.max().unwrap(), original.max().unwrap());
    }

    /// A truncated or foreign payload must fail rather than decode into a
    /// sketch whose answers are quietly wrong.
    #[test]
    fn wire_rejects_a_payload_it_did_not_write() {
        let proto = SortKeySketchState {
            k: 800,
            null_count: 0,
            key_min: vec![],
            key_max: vec![],
            levels: b"not an arrow stream".to_vec(),
        };
        let err = SortKeySketch::try_from_proto(&proto, SortOptions::default())
            .expect_err("a non-IPC payload must not decode");
        assert!(!err.to_string().is_empty());
    }
}
