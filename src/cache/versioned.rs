use bincode::Options;
use serde::{Serialize, de::DeserializeOwned};
use tracing::warn;

use crate::errors::PersistenceError;

const VERSION_BYTES: usize = 4;

pub(crate) fn encode<T: Serialize>(
    magic: &[u8; 8],
    version: u32,
    value: &T,
    label: &'static str,
) -> Result<Vec<u8>, PersistenceError> {
    let payload =
        bincode::serialize(value).map_err(|err| PersistenceError::serialize(label, err))?;
    let mut data = Vec::with_capacity(magic.len() + VERSION_BYTES + payload.len());
    data.extend_from_slice(magic);
    data.extend_from_slice(&version.to_le_bytes());
    data.extend_from_slice(&payload);
    Ok(data)
}

pub(crate) fn decode<T: DeserializeOwned>(
    data: &[u8],
    magic: &[u8; 8],
    version: u32,
    label: &'static str,
) -> Option<T> {
    let header_len = magic.len() + VERSION_BYTES;
    if data.len() < header_len {
        warn!(
            cache = label,
            bytes = data.len(),
            "Cache file is missing version header; treating as cache miss"
        );
        return None;
    }

    if &data[..magic.len()] != magic {
        warn!(
            cache = label,
            "Cache file has unrecognized magic header; treating as cache miss"
        );
        return None;
    }

    let version_start = magic.len();
    let found_version = u32::from_le_bytes(
        data[version_start..header_len]
            .try_into()
            .expect("version slice length is fixed"),
    );
    if found_version != version {
        warn!(
            cache = label,
            expected = version,
            found = found_version,
            "Cache file version mismatch; treating as cache miss"
        );
        return None;
    }

    let payload = &data[header_len..];
    let mut cursor = std::io::Cursor::new(payload);
    let value = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(payload.len() as u64)
        .deserialize_from(&mut cursor)
        .inspect_err(|e| warn!(cache = label, error = %e, "Failed to parse cache payload"))
        .ok()?;
    if cursor.position() != payload.len() as u64 {
        warn!(
            cache = label,
            consumed = cursor.position(),
            bytes = payload.len(),
            "Cache payload has trailing bytes; treating as cache miss"
        );
        return None;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAGIC: &[u8; 8] = b"VERSION\0";

    #[test]
    fn exact_payload_round_trips() {
        let encoded = encode(MAGIC, 1, &vec![1_u64, 2, 3], "test").expect("encode");
        assert_eq!(
            decode::<Vec<u64>>(&encoded, MAGIC, 1, "test"),
            Some(vec![1, 2, 3])
        );
    }

    #[test]
    fn trailing_payload_bytes_are_rejected() {
        let mut encoded = encode(MAGIC, 1, &42_u64, "test").expect("encode");
        encoded.push(0xff);
        assert_eq!(decode::<u64>(&encoded, MAGIC, 1, "test"), None);
    }
}
