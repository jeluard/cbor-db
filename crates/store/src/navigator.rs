//! Zero-copy CBOR navigator for runtime path resolution.
//!
//! This module provides fast navigation through CBOR-encoded data
//! using precomputed array indices from the CDDL schema.
//!
//! The navigator operates on raw byte slices without allocation,
//! returning sub-slices pointing into the original data.

use minicbor::decode::{Decoder, Error as DecodeError};

/// Navigate through CBOR data following a sequence of array indices.
///
/// # Arguments
/// * `data` - Raw CBOR-encoded bytes
/// * `indices` - Array of indices to follow (e.g., `[0, 1, 2]` means
///   enter first array, get element 1, then get element 2 from that)
///
/// # Returns
/// A slice pointing to the target element within `data`, or an error
/// if navigation fails.
///
/// # Example
/// ```ignore
/// // Navigate to the 3rd element of the 1st element of the root array
/// let target = navigate(data, &[0, 2])?;
/// ```
pub fn navigate<'a>(data: &'a [u8], indices: &[usize]) -> Result<&'a [u8], NavigatorError> {
    if indices.is_empty() {
        return Ok(data);
    }

    let mut decoder = Decoder::new(data);

    for (depth, &target_idx) in indices.iter().enumerate() {
        // Expect an array at current position
        let array_len = decoder
            .array()
            .map_err(|e| NavigatorError::DecodeError {
                depth,
                index: target_idx,
                source: e,
            })?
            .ok_or(NavigatorError::IndefiniteArray { depth })?;

        if target_idx >= array_len as usize {
            return Err(NavigatorError::IndexOutOfBounds {
                depth,
                index: target_idx,
                array_len: array_len as usize,
            });
        }

        // Skip elements until we reach target_idx
        for skip_idx in 0..target_idx {
            decoder.skip().map_err(|e| NavigatorError::SkipError {
                depth,
                index: skip_idx,
                source: e,
            })?;
        }

        // If this is not the last index, we'll continue into the next array
        // If it is the last index, we'll return this element's slice
        if depth == indices.len() - 1 {
            // This is the target element - find its end
            let element_start = decoder.position();
            decoder.skip().map_err(|e| NavigatorError::SkipError {
                depth,
                index: target_idx,
                source: e,
            })?;
            let element_end = decoder.position();

            return Ok(&data[element_start..element_end]);
        }
    }

    unreachable!("navigate should return from the final depth")
}

/// Navigate and return the start offset and length of the target element.
///
/// This is useful when you need the position info rather than a slice.
pub fn navigate_to_offset(
    data: &[u8],
    indices: &[usize],
) -> Result<(usize, usize), NavigatorError> {
    if indices.is_empty() {
        return Ok((0, data.len()));
    }

    let slice = navigate(data, indices)?;

    // Calculate offset from pointer difference
    let start = slice.as_ptr() as usize - data.as_ptr() as usize;
    let len = slice.len();

    Ok((start, len))
}

pub fn take_cbor_value(data: &[u8]) -> Result<&[u8], NavigatorError> {
    let (start, len) = take_cbor_value_to_offset(data)?;
    Ok(&data[start..start + len])
}

pub fn take_cbor_value_to_offset(data: &[u8]) -> Result<(usize, usize), NavigatorError> {
    let mut decoder = Decoder::new(data);
    let start = decoder.position();
    decoder.skip().map_err(|source| NavigatorError::SkipError {
        depth: 0,
        index: 0,
        source,
    })?;
    let end = decoder.position();
    Ok((start, end - start))
}

/// Errors that can occur during CBOR navigation
#[derive(Debug)]
pub enum NavigatorError {
    /// Failed to decode CBOR at a specific depth/index
    DecodeError {
        depth: usize,
        index: usize,
        source: DecodeError,
    },
    /// Encountered an indefinite-length array (not supported for indexed access)
    IndefiniteArray { depth: usize },
    /// Array index out of bounds
    IndexOutOfBounds {
        depth: usize,
        index: usize,
        array_len: usize,
    },
    /// Error while skipping over an element
    SkipError {
        depth: usize,
        index: usize,
        source: DecodeError,
    },
}

impl std::fmt::Display for NavigatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeError {
                depth,
                index,
                source,
            } => {
                write!(
                    f,
                    "CBOR decode error at depth {}, index {}: {}",
                    depth, index, source
                )
            }
            Self::IndefiniteArray { depth } => {
                write!(
                    f,
                    "Indefinite-length array at depth {} (not supported)",
                    depth
                )
            }
            Self::IndexOutOfBounds {
                depth,
                index,
                array_len,
            } => {
                write!(
                    f,
                    "Index {} out of bounds for array of length {} at depth {}",
                    index, array_len, depth
                )
            }
            Self::SkipError {
                depth,
                index,
                source,
            } => {
                write!(
                    f,
                    "Error skipping element {} at depth {}: {}",
                    index, depth, source
                )
            }
        }
    }
}

impl std::error::Error for NavigatorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DecodeError { source, .. } | Self::SkipError { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minicbor::encode::Encoder;

    fn encode_test_data() -> Vec<u8> {
        // Encode: [[1, 2, 3], [4, 5], 6]
        let mut buf = Vec::new();
        let mut encoder = Encoder::new(&mut buf);

        encoder.array(3).unwrap();
        {
            encoder.array(3).unwrap();
            encoder.u8(1).unwrap();
            encoder.u8(2).unwrap();
            encoder.u8(3).unwrap();
        }
        {
            encoder.array(2).unwrap();
            encoder.u8(4).unwrap();
            encoder.u8(5).unwrap();
        }
        encoder.u8(6).unwrap();

        buf
    }

    fn encode_nested_skip_data() -> Vec<u8> {
        // Encode: [ [1, [2, 3]], {4: [5, 6]}, tagged(7), 8 ]
        let mut buf = Vec::new();
        let mut encoder = Encoder::new(&mut buf);

        encoder.array(4).unwrap();

        encoder.array(2).unwrap();
        encoder.u8(1).unwrap();
        encoder.array(2).unwrap();
        encoder.u8(2).unwrap();
        encoder.u8(3).unwrap();

        encoder.map(1).unwrap();
        encoder.u8(4).unwrap();
        encoder.array(2).unwrap();
        encoder.u8(5).unwrap();
        encoder.u8(6).unwrap();

        encoder.tag(minicbor::data::Tag::new(24)).unwrap();
        encoder.u8(7).unwrap();

        encoder.u8(8).unwrap();

        buf
    }

    fn encode_indefinite_skip_data() -> Vec<u8> {
        // Encode: [ [_ 1, [2, 3]], 9 ]
        vec![0x82, 0x9f, 0x01, 0x82, 0x02, 0x03, 0xff, 0x09]
    }

    #[test]
    fn test_navigate_simple() {
        let data = encode_test_data();

        // Navigate to root element 2 (the value 6)
        let result = navigate(&data, &[2]).unwrap();
        assert_eq!(result, &[0x06]); // CBOR encoding of 6

        // Navigate to element [0][1] (the value 2)
        let result = navigate(&data, &[0, 1]).unwrap();
        assert_eq!(result, &[0x02]); // CBOR encoding of 2

        // Navigate to element [1][0] (the value 4)
        let result = navigate(&data, &[1, 0]).unwrap();
        assert_eq!(result, &[0x04]); // CBOR encoding of 4
    }

    #[test]
    fn test_navigate_to_array() {
        let data = encode_test_data();

        // Navigate to element [0] (the array [1, 2, 3])
        let result = navigate(&data, &[0]).unwrap();

        // Verify it's an array of 3 elements
        let mut decoder = Decoder::new(result);
        assert_eq!(decoder.array().unwrap(), Some(3));
    }

    #[test]
    fn test_navigate_out_of_bounds() {
        let data = encode_test_data();

        // Try to access index 10 in root array (only has 3 elements)
        let result = navigate(&data, &[10]);
        assert!(matches!(
            result,
            Err(NavigatorError::IndexOutOfBounds {
                index: 10,
                array_len: 3,
                ..
            })
        ));
    }

    #[test]
    fn test_navigate_empty_indices() {
        let data = encode_test_data();

        // Empty indices should return the whole data
        let result = navigate(&data, &[]).unwrap();
        assert_eq!(result.len(), data.len());
    }

    #[test]
    fn test_navigate_to_offset() {
        let data = encode_test_data();

        let (offset, len) = navigate_to_offset(&data, &[2]).unwrap();
        assert_eq!(&data[offset..offset + len], &[0x06]);
    }

    #[test]
    fn takes_the_current_value_slice() {
        let data = encode_test_data();
        let slice = take_cbor_value(&data[1..]).unwrap();

        let mut decoder = Decoder::new(slice);
        assert_eq!(decoder.array().unwrap(), Some(3));
    }

    #[test]
    fn test_navigate_skips_nested_values() {
        let data = encode_nested_skip_data();

        let result = navigate(&data, &[3]).unwrap();

        assert_eq!(result, &[0x08]);
    }

    #[test]
    fn test_navigate_skips_indefinite_array_value() {
        let data = encode_indefinite_skip_data();

        let result = navigate(&data, &[1]).unwrap();

        assert_eq!(result, &[0x09]);
    }
}
