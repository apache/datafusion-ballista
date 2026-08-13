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

//! Order-preserving `u64` encoding of a single fixed-width `ORDER BY` key,
//! so [`crate::kll::KllSketch`] can sketch it without knowing the column's
//! type or sort direction.
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
//! Encoding skips NULLs entirely; callers track the count separately. A
//! NULL has no position among the values, only a side, and `nulls_first` /
//! `nulls_last` says which — that is one bit of plan-time information, not
//! something the sketch needs to carry. Keeping it out of the key leaves
//! the key 8 bytes wide rather than 16, which the same benchmark measured
//! at 1.23× versus 1.58×.
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
//! Exactness matters most at the extremes. A sketch's `min` and `max` are
//! what decide which shuffle files overlap which output partition, so an
//! extreme that gets dropped drops rows with it. Keys compare with `Ord`
//! over every element, which has no value it silently ignores.
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

use datafusion::arrow::array::{Array, ArrowPrimitiveType, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::{Result, ScalarValue, internal_datafusion_err};

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

/// Encodes one fixed-width `ORDER BY` key to `u64` and back. See the module
/// docs for the encoding and for why NULLs are handled out of band.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKeyCodec {
    /// The column's full arrow type, retained so [`Self::decode`] can
    /// rebuild a `ScalarValue` that keeps the parts the key doesn't carry
    /// — a `Timestamp`'s timezone above all.
    data_type: DataType,
    /// `true` inverts every key bit, reversing the sketch's ascending order
    /// into the `DESC` order the plan asked for.
    descending: bool,
}

impl SortKeyCodec {
    /// Build a codec for `data_type`, or `None` if this module doesn't
    /// encode that type and the caller should fall back to arrow-row.
    pub fn try_new(data_type: &DataType, descending: bool) -> Option<Self> {
        macro_rules! supported {
            ($arrow_type:ty) => {
                true
            };
        }
        let supported = dispatch_sortable!(data_type, supported, false);
        supported.then(|| Self {
            data_type: data_type.clone(),
            descending,
        })
    }

    /// The arrow type this codec was built for.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
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
        let descending = self.descending;
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
        let key = if self.descending { !key } else { key };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kll::KllSketch;
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
        let codec = SortKeyCodec::try_new(&DataType::Int64, false).unwrap();
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
        let codec = SortKeyCodec::try_new(&DataType::Int64, true).unwrap();
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
        let codec = SortKeyCodec::try_new(&DataType::Float64, false).unwrap();
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

        let codec = SortKeyCodec::try_new(&DataType::Float64, false).unwrap();
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

        let codec = SortKeyCodec::try_new(&DataType::Float64, false).unwrap();
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
        let codec = SortKeyCodec::try_new(&DataType::Float64, false).unwrap();
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
        let codec = SortKeyCodec::try_new(&DataType::Float64, false).unwrap();
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
        let codec = SortKeyCodec::try_new(&DataType::Float64, false).unwrap();
        let nan_batch = vec![f64::NAN, -f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        let plain_batch = vec![1.0, 5.0];

        let nan_first =
            sketch_batches(&codec, vec![nan_batch.clone(), plain_batch.clone()]);
        let nan_second = sketch_batches(&codec, vec![plain_batch, nan_batch]);

        assert_eq!(nan_first.min(), nan_second.min(), "min is order dependent");
        assert_eq!(nan_first.max(), nan_second.max(), "max is order dependent");
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
        let codec = SortKeyCodec::try_new(&DataType::UInt64, false).unwrap();
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
        let codec = SortKeyCodec::try_new(&data_type, false).unwrap();
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

    /// NULLs leave the key stream entirely, so the caller can count them
    /// separately and place them by `nulls_first` at routing time.
    #[test]
    fn encode_skips_nulls_and_keeps_value_order() {
        let array = Int64Array::from(vec![Some(3), None, Some(1), None, Some(2)]);
        let codec = SortKeyCodec::try_new(&DataType::Int64, false).unwrap();
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
                SortKeyCodec::try_new(&data_type, false).is_none(),
                "{data_type:?} must fall through to the arrow-row path"
            );
        }
    }

    /// A codec must reject an array of the wrong type instead of
    /// reinterpreting its bits.
    #[test]
    fn encode_rejects_mismatched_array_type() {
        let codec = SortKeyCodec::try_new(&DataType::Int64, false).unwrap();
        let array: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let err = codec
            .encode(array.as_ref())
            .expect_err("type mismatch must not silently reinterpret");
        assert!(err.to_string().contains("built for"), "got: {err}");
    }
}
