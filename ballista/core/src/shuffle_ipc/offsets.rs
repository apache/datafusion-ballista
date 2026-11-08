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

//! Offsets stored as lengths inside a record-batch message body.
//!
//! The rewrite is exact for any offsets, including ones that do not start at
//! zero: the first entry is kept as is (a length from zero), and wrapping
//! arithmetic makes every bit pattern round-trip.
//!
//! The lengths are also split into byte planes: byte `b` of entry `i` moves to
//! `b * count + i`. Lengths are small, so their high planes are all zero and
//! compress to almost nothing.
//!
//! Ported from DataFusion Comet's `offsets_as_lengths.rs`
//! (apache/datafusion-comet#6943).

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::error::ArrowError;

/// What a record-batch body buffer holds, as far as this rewrite is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BufferRole {
    Other,
    Offsets32,
    Offsets64,
}

/// The role of every buffer `arrow::ipc::writer` emits for a record batch of
/// `schema`, in body order (metadata version V5). `None` when some type's
/// layout is not mapped here: views, list views, unions, run-end encoding and
/// dictionaries, whose buffer counts depend on the data or which need
/// dictionary messages.
pub(crate) fn buffer_roles(schema: &Schema) -> Option<Vec<BufferRole>> {
    let mut roles = Vec::new();
    for field in schema.fields() {
        push_roles(field.data_type(), &mut roles)?;
    }
    Some(roles)
}

/// Mirrors `write_array_data` in arrow-ipc: a validity buffer for every type
/// except null (and unions and run-end encoding, which are not mapped), then
/// the type's own buffers, then its children.
fn push_roles(data_type: &DataType, roles: &mut Vec<BufferRole>) -> Option<()> {
    use BufferRole::*;
    match data_type {
        DataType::Null => {}
        DataType::Boolean => roles.extend([Other, Other]),
        DataType::Utf8 | DataType::Binary => roles.extend([Other, Offsets32, Other]),
        DataType::LargeUtf8 | DataType::LargeBinary => {
            roles.extend([Other, Offsets64, Other])
        }
        DataType::List(child) | DataType::Map(child, _) => {
            roles.extend([Other, Offsets32]);
            push_roles(child.data_type(), roles)?;
        }
        DataType::LargeList(child) => {
            roles.extend([Other, Offsets64]);
            push_roles(child.data_type(), roles)?;
        }
        DataType::FixedSizeList(child, _) => {
            roles.push(Other);
            push_roles(child.data_type(), roles)?;
        }
        DataType::Struct(fields) => {
            roles.push(Other);
            for field in fields {
                push_roles(field.data_type(), roles)?;
            }
        }
        data_type
            if data_type.is_numeric()
                || data_type.is_temporal()
                || matches!(data_type, DataType::FixedSizeBinary(_)) =>
        {
            roles.extend([Other, Other])
        }
        _ => return None,
    }
    Some(())
}

/// Whether a batch with these roles has anything to rewrite.
pub(crate) fn has_offsets(roles: &[BufferRole]) -> bool {
    roles.iter().any(|role| *role != BufferRole::Other)
}

/// The (offset, length) of every body buffer of a record-batch message, in
/// body order. A value that does not fit a `usize` becomes one that lies
/// outside any body, so the range check that follows rejects it.
pub(crate) fn buffer_ranges(
    batch: &datafusion::arrow::ipc::RecordBatch<'_>,
) -> Result<Vec<(usize, usize)>, ArrowError> {
    let buffers = batch.buffers().ok_or_else(|| {
        ArrowError::IpcError("Shuffle record batch message has no buffers".to_string())
    })?;
    let range = |value: i64| usize::try_from(value).unwrap_or(usize::MAX);
    Ok(buffers
        .iter()
        .map(|buffer| (range(buffer.offset()), range(buffer.length())))
        .collect())
}

/// Rewrites every offsets buffer of `body` as byte planes of lengths, in place.
/// `buffers` are the record-batch message's (offset, length) pairs in body
/// order, one per entry of `roles`.
pub(crate) fn offsets_to_lengths(
    body: &mut [u8],
    buffers: &[(usize, usize)],
    roles: &[BufferRole],
) -> Result<(), ArrowError> {
    rewrite(
        body,
        buffers,
        roles,
        to_length_planes_32,
        to_length_planes_64,
    )
}

/// Undoes [`offsets_to_lengths`], in place.
pub(crate) fn lengths_to_offsets(
    body: &mut [u8],
    buffers: &[(usize, usize)],
    roles: &[BufferRole],
) -> Result<(), ArrowError> {
    rewrite(
        body,
        buffers,
        roles,
        from_length_planes_32,
        from_length_planes_64,
    )
}

fn rewrite(
    body: &mut [u8],
    buffers: &[(usize, usize)],
    roles: &[BufferRole],
    rewrite32: fn(&mut [u8], &mut Vec<u32>),
    rewrite64: fn(&mut [u8], &mut Vec<u64>),
) -> Result<(), ArrowError> {
    if buffers.len() != roles.len() {
        return Err(ArrowError::IpcError(format!(
            "Shuffle record batch has {} body buffers where its schema has {}",
            buffers.len(),
            roles.len()
        )));
    }
    let (mut scratch32, mut scratch64) = (Vec::new(), Vec::new());
    for (&(offset, len), role) in buffers.iter().zip(roles) {
        match role {
            BufferRole::Other => {}
            BufferRole::Offsets32 => {
                rewrite32(buffer_mut(body, offset, len)?, &mut scratch32)
            }
            BufferRole::Offsets64 => {
                rewrite64(buffer_mut(body, offset, len)?, &mut scratch64)
            }
        }
    }
    Ok(())
}

/// `body[offset..offset + len]`, or an error when that range is not inside
/// `body`.
fn buffer_mut(
    body: &mut [u8],
    offset: usize,
    len: usize,
) -> Result<&mut [u8], ArrowError> {
    let body_len = body.len();
    offset
        .checked_add(len)
        .and_then(|end| body.get_mut(offset..end))
        .ok_or_else(|| {
            ArrowError::IpcError(format!(
                "Shuffle offsets buffer at {offset} with length {len} is outside its \
                 {body_len}-byte body"
            ))
        })
}

/// Lengths in byte planes for one offset width. The first entry is a length
/// from zero. One pass computes the lengths, then one contiguous pass per plane
/// moves their bytes, so the loops vectorize; decoding gathers the planes back
/// the same way and keeps a running sum. Only whole entries move: the trailing
/// bytes of a buffer that is not a whole number of entries stay where they are.
macro_rules! length_planes {
    ($to_planes:ident, $from_planes:ident, $word:ty) => {
        fn $to_planes(bytes: &mut [u8], scratch: &mut Vec<$word>) {
            const WIDTH: usize = size_of::<$word>();
            let count = bytes.len() / WIDTH;
            if count == 0 {
                return;
            }
            let entries = &mut bytes[..count * WIDTH];
            let words = entries.as_chunks::<WIDTH>().0;
            scratch.clear();
            scratch.reserve(count);
            scratch.push(<$word>::from_le_bytes(words[0]));
            scratch.extend(words.windows(2).map(|pair| {
                <$word>::from_le_bytes(pair[1])
                    .wrapping_sub(<$word>::from_le_bytes(pair[0]))
            }));
            for (plane, out) in entries.chunks_exact_mut(count).enumerate() {
                let shift = 8 * plane as u32;
                for (byte, length) in out.iter_mut().zip(scratch.iter()) {
                    *byte = (*length >> shift) as u8;
                }
            }
        }

        fn $from_planes(bytes: &mut [u8], scratch: &mut Vec<$word>) {
            const WIDTH: usize = size_of::<$word>();
            let count = bytes.len() / WIDTH;
            if count == 0 {
                return;
            }
            let entries = &mut bytes[..count * WIDTH];
            scratch.clear();
            scratch.resize(count, 0);
            for (plane, input) in entries.chunks_exact(count).enumerate() {
                let shift = 8 * plane as u32;
                for (length, byte) in scratch.iter_mut().zip(input) {
                    *length |= <$word>::from(*byte) << shift;
                }
            }
            let mut running: $word = 0;
            let words = entries.as_chunks_mut::<WIDTH>().0;
            for (entry, length) in words.iter_mut().zip(scratch.iter()) {
                running = running.wrapping_add(*length);
                *entry = running.to_le_bytes();
            }
        }
    };
}

length_planes!(to_length_planes_32, from_length_planes_32, u32);
length_planes!(to_length_planes_64, from_length_planes_64, u64);

#[cfg(test)]
mod tests {
    use super::*;
    use BufferRole::*;
    use datafusion::arrow::datatypes::{Field, Fields, TimeUnit};
    use std::sync::Arc;

    fn bytes_i32(values: &[i32]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn bytes_i64(values: &[i64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[test]
    fn lengths_round_trip_any_bit_pattern() {
        let offsets32 = [0, 3, 3, 9, i32::MAX, i32::MIN, -1, 7];
        let offsets64 = [5, 5, i64::MAX, i64::MIN, 0, -12];
        let mut body = bytes_i32(&offsets32);
        body.extend(bytes_i64(&offsets64));
        let original = body.clone();
        let buffers = [(0, 32), (32, 48)];

        offsets_to_lengths(&mut body, &buffers, &[Offsets32, Offsets64]).unwrap();
        // lengths 0, 3, 0, 6, ... in byte planes: the low bytes of all 8
        // entries come first
        assert_eq!(body[..4], [0, 3, 0, 6]);
        // the 64-bit buffer's first entry is kept (a length from zero)
        assert_eq!(body[32], 5);
        assert_ne!(body, original);

        lengths_to_offsets(&mut body, &buffers, &[Offsets32, Offsets64]).unwrap();
        assert_eq!(body, original);
    }

    #[test]
    fn other_buffers_and_trailing_bytes_are_untouched() {
        let mut body = bytes_i32(&[0, 4, 8]);
        body.extend([1, 2, 3]); // not a whole entry
        let other = body.clone();
        body.extend(other.clone());
        let buffers = [(0, 15), (15, 15)];

        offsets_to_lengths(&mut body, &buffers, &[Offsets32, Other]).unwrap();
        // lengths 0, 4, 4: one plane of low bytes, then three planes of zeros
        assert_eq!(&body[..12], &[0, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&body[12..15], &[1, 2, 3]);
        assert_eq!(&body[15..], other.as_slice());
        lengths_to_offsets(&mut body, &buffers, &[Offsets32, Other]).unwrap();
        assert_eq!(&body[..12], bytes_i32(&[0, 4, 8]).as_slice());
    }

    #[test]
    fn out_of_range_buffers_are_errors() {
        let mut body = vec![0u8; 16];
        for buffers in [[(8, 12)], [(usize::MAX, 2)]] {
            assert!(lengths_to_offsets(&mut body, &buffers, &[Offsets32]).is_err());
        }
        // an Other buffer is never touched, so its range is not checked here
        assert!(lengths_to_offsets(&mut body, &[(8, 12)], &[Other]).is_ok());
        let error = lengths_to_offsets(&mut body, &[(0, 4)], &[]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("1 body buffers where its schema has 0")
        );
    }

    #[test]
    fn roles_follow_the_ipc_buffer_order() {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        ));
        let schema = Schema::new(vec![
            Field::new("n", DataType::Null, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
            Field::new("d", DataType::Decimal128(10, 2), true),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("f", DataType::FixedSizeBinary(3), true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeBinary, true),
            Field::new("l", DataType::List(Arc::clone(&item)), true),
            Field::new("ll", DataType::LargeList(Arc::clone(&item)), true),
            Field::new("fl", DataType::FixedSizeList(Arc::clone(&item), 2), true),
            Field::new("m", DataType::Map(entries, false), true),
            Field::new(
                "st",
                DataType::Struct(Fields::from(vec![Field::new(
                    "x",
                    DataType::Utf8,
                    true,
                )])),
                true,
            ),
        ]);
        let roles = buffer_roles(&schema).unwrap();
        #[rustfmt::skip]
        let expected = vec![
            // n: none
            Other, Other, // b
            Other, Other, // i
            Other, Other, // d
            Other, Other, // t
            Other, Other, // f
            Other, Offsets32, Other, // s
            Other, Offsets64, Other, // ls
            Other, Offsets32, Other, Offsets32, Other, // l<utf8>
            Other, Offsets64, Other, Offsets32, Other, // ll<utf8>
            Other, Other, Offsets32, Other, // fl<utf8>
            Other, Offsets32, Other, Other, Offsets32, Other, Other, Other, // m<utf8, i64>
            Other, Other, Offsets32, Other, // st{utf8}
        ];
        assert_eq!(roles, expected);
        assert!(has_offsets(&roles));
    }

    #[test]
    fn unmapped_layouts_have_no_roles() {
        for data_type in [
            DataType::Utf8View,
            DataType::BinaryView,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
        ] {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", data_type.clone(), true),
            ]);
            assert_eq!(buffer_roles(&schema), None, "{data_type}");
        }
        let numeric = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        assert!(!has_offsets(&buffer_roles(&numeric).unwrap()));
    }
}
