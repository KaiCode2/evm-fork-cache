use serde::{Serialize, de::DeserializeOwned};
use tracing::warn;

use anyhow::{Context as _, Result};

const VERSION_BYTES: usize = 4;

pub(crate) fn encode<T: Serialize>(
    magic: &[u8; 8],
    version: u32,
    value: &T,
    label: &'static str,
) -> Result<Vec<u8>> {
    let payload =
        bincode::serialize(value).with_context(|| format!("failed to serialize {label}"))?;
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

    bincode::deserialize(&data[header_len..])
        .inspect_err(|e| warn!(cache = label, error = %e, "Failed to parse cache payload"))
        .ok()
}
