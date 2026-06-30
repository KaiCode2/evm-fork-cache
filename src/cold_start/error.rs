//! The typed error surface for cold-start runs.

/// A hard error that aborts a cold-start round or run.
///
/// Deliberately typed (no `#[from] anyhow::Error` blanket arm): composed-primitive
/// errors are converted explicitly at call sites via [`ColdStartError::Fetch`], so
/// a partial round's outcomes are never silently collapsed.
#[derive(Debug, thiserror::Error)]
pub enum ColdStartError {
    /// A round declared verify/probe slots but the cache has no storage batch
    /// fetcher configured.
    #[error("cold-start requires a storage batch fetcher")]
    NoBatchFetcher,
    /// The planner kept returning `Continue` past `max_rounds` executed rounds.
    #[error("cold-start round budget exceeded ({max_rounds})")]
    RoundBudgetExceeded {
        /// The configured maximum number of executed rounds.
        max_rounds: usize,
    },
    /// A composed fetch/call error, carrying the underlying cause explicitly.
    #[error("cold-start fetch error: {0}")]
    Fetch(anyhow::Error),
}
