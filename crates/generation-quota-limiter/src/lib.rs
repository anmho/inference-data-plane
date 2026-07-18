use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaLimits {
    pub requests_per_minute: u64,
    pub prompt_tokens_per_minute: u64,
    pub generated_tokens_per_minute: u64,
}

impl Default for QuotaLimits {
    fn default() -> Self {
        Self {
            requests_per_minute: 60,
            prompt_tokens_per_minute: 120_000,
            generated_tokens_per_minute: 60_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaCheck {
    pub api_key: String,
    pub prompt_tokens: u64,
    pub reserved_generated_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuotaReservation {
    pub id: Uuid,
    pub api_key: String,
    pub prompt_tokens: u64,
    pub reserved_generated_tokens: u64,
    pub remaining_requests: u64,
    pub remaining_prompt_tokens: u64,
    pub remaining_generated_tokens: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QuotaError {
    #[error("request quota exceeded: remaining={remaining}, requested=1")]
    RequestsExceeded { remaining: u64 },
    #[error("prompt token quota exceeded: remaining={remaining}, requested={requested}")]
    PromptTokensExceeded { remaining: u64, requested: u64 },
    #[error("generated token quota exceeded: remaining={remaining}, requested={requested}")]
    GeneratedTokensExceeded { remaining: u64, requested: u64 },
    #[error("reservation not found")]
    ReservationNotFound,
}

#[derive(Debug, Clone)]
pub struct InMemoryQuotaLimiter {
    limits: QuotaLimits,
    state: Arc<Mutex<HashMap<String, WindowCounters>>>,
    reservations: Arc<Mutex<HashMap<Uuid, QuotaReservation>>>,
}

#[derive(Debug, Clone, Default)]
struct WindowCounters {
    window_start_epoch_minute: u64,
    requests: u64,
    prompt_tokens: u64,
    generated_tokens_reserved: u64,
}

impl InMemoryQuotaLimiter {
    pub fn new(limits: QuotaLimits) -> Self {
        Self {
            limits,
            state: Arc::new(Mutex::new(HashMap::new())),
            reservations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn reserve(&self, check: QuotaCheck) -> Result<QuotaReservation, QuotaError> {
        let minute = epoch_minute();
        let mut state = self.state.lock().await;
        let counters = state.entry(check.api_key.clone()).or_default();
        if counters.window_start_epoch_minute != minute {
            *counters = WindowCounters {
                window_start_epoch_minute: minute,
                ..WindowCounters::default()
            };
        }

        let remaining_requests = self
            .limits
            .requests_per_minute
            .saturating_sub(counters.requests);
        if remaining_requests < 1 {
            return Err(QuotaError::RequestsExceeded {
                remaining: remaining_requests,
            });
        }

        let remaining_prompt = self
            .limits
            .prompt_tokens_per_minute
            .saturating_sub(counters.prompt_tokens);
        if remaining_prompt < check.prompt_tokens {
            return Err(QuotaError::PromptTokensExceeded {
                remaining: remaining_prompt,
                requested: check.prompt_tokens,
            });
        }

        let remaining_generated = self
            .limits
            .generated_tokens_per_minute
            .saturating_sub(counters.generated_tokens_reserved);
        if remaining_generated < check.reserved_generated_tokens {
            return Err(QuotaError::GeneratedTokensExceeded {
                remaining: remaining_generated,
                requested: check.reserved_generated_tokens,
            });
        }

        counters.requests += 1;
        counters.prompt_tokens += check.prompt_tokens;
        counters.generated_tokens_reserved += check.reserved_generated_tokens;

        let reservation = QuotaReservation {
            id: Uuid::new_v4(),
            api_key: check.api_key,
            prompt_tokens: check.prompt_tokens,
            reserved_generated_tokens: check.reserved_generated_tokens,
            remaining_requests: self
                .limits
                .requests_per_minute
                .saturating_sub(counters.requests),
            remaining_prompt_tokens: self
                .limits
                .prompt_tokens_per_minute
                .saturating_sub(counters.prompt_tokens),
            remaining_generated_tokens: self
                .limits
                .generated_tokens_per_minute
                .saturating_sub(counters.generated_tokens_reserved),
        };

        self.reservations
            .lock()
            .await
            .insert(reservation.id, reservation.clone());
        Ok(reservation)
    }

    pub async fn reconcile(
        &self,
        reservation_id: Uuid,
        actual_generated_tokens: u64,
    ) -> Result<u64, QuotaError> {
        let reservation = self
            .reservations
            .lock()
            .await
            .remove(&reservation_id)
            .ok_or(QuotaError::ReservationNotFound)?;

        let refund = reservation
            .reserved_generated_tokens
            .saturating_sub(actual_generated_tokens);
        if refund == 0 {
            return Ok(0);
        }

        let mut state = self.state.lock().await;
        if let Some(counters) = state.get_mut(&reservation.api_key) {
            counters.generated_tokens_reserved =
                counters.generated_tokens_reserved.saturating_sub(refund);
        }
        Ok(refund)
    }

    pub async fn cancel(&self, reservation_id: Uuid) -> Result<u64, QuotaError> {
        self.reconcile(reservation_id, 0).await
    }
}

fn epoch_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        / 60
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter() -> InMemoryQuotaLimiter {
        InMemoryQuotaLimiter::new(QuotaLimits {
            requests_per_minute: 2,
            prompt_tokens_per_minute: 100,
            generated_tokens_per_minute: 50,
        })
    }

    #[tokio::test]
    async fn reserves_and_refunds_unused_generation_tokens() {
        let limiter = limiter();
        let reservation = limiter
            .reserve(QuotaCheck {
                api_key: "key".to_string(),
                prompt_tokens: 10,
                reserved_generated_tokens: 40,
            })
            .await
            .unwrap();

        let refunded = limiter.reconcile(reservation.id, 12).await.unwrap();

        assert_eq!(refunded, 28);
    }

    #[tokio::test]
    async fn rejects_when_generated_quota_would_be_exceeded() {
        let limiter = limiter();
        limiter
            .reserve(QuotaCheck {
                api_key: "key".to_string(),
                prompt_tokens: 10,
                reserved_generated_tokens: 40,
            })
            .await
            .unwrap();

        let error = limiter
            .reserve(QuotaCheck {
                api_key: "key".to_string(),
                prompt_tokens: 10,
                reserved_generated_tokens: 20,
            })
            .await
            .unwrap_err();

        assert_eq!(
            error,
            QuotaError::GeneratedTokensExceeded {
                remaining: 10,
                requested: 20
            }
        );
    }

    #[tokio::test]
    async fn cancellation_refunds_reserved_generation_tokens() {
        let limiter = limiter();
        let reservation = limiter
            .reserve(QuotaCheck {
                api_key: "key".to_string(),
                prompt_tokens: 10,
                reserved_generated_tokens: 40,
            })
            .await
            .unwrap();

        let refunded = limiter.cancel(reservation.id).await.unwrap();

        assert_eq!(refunded, 40);
    }
}
