use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Prompt {
    Text(String),
    Chat(Vec<ChatMessage>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelBudget {
    pub model: String,
    pub context_limit: usize,
    pub default_max_tokens: usize,
    pub max_output_tokens: usize,
}

impl ModelBudget {
    pub fn local_vllm(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            context_limit: 4096,
            default_max_tokens: 256,
            max_output_tokens: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BudgetPolicy {
    StrictReject,
    TruncateOldest,
    ReserveOutputFirst,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BudgetRequest {
    pub prompt: Prompt,
    pub requested_max_tokens: Option<usize>,
    pub policy: BudgetPolicy,
    pub model: ModelBudget,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BudgetDecision {
    pub admitted: bool,
    pub reason: String,
    pub model: String,
    pub prompt_tokens: usize,
    pub requested_max_tokens: usize,
    pub admitted_max_tokens: usize,
    pub available_generation_tokens: usize,
    pub truncated_messages: usize,
    pub prompt: Prompt,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BudgetError {
    #[error("model context limit must be greater than zero")]
    InvalidContextLimit,
    #[error("model max output tokens must be greater than zero")]
    InvalidMaxOutputTokens,
}

pub trait TokenCounter: Send + Sync {
    fn count_prompt(&self, prompt: &Prompt) -> usize;
    fn count_text(&self, text: &str) -> usize;
}

#[derive(Debug, Default)]
pub struct ApproxTokenCounter;

impl TokenCounter for ApproxTokenCounter {
    fn count_prompt(&self, prompt: &Prompt) -> usize {
        match prompt {
            Prompt::Text(text) => self.count_text(text),
            Prompt::Chat(messages) => {
                messages
                    .iter()
                    .map(|message| {
                        self.count_text(&message.role) + self.count_text(&message.content) + 4
                    })
                    .sum::<usize>()
                    + 2
            }
        }
    }

    fn count_text(&self, text: &str) -> usize {
        let words = text.split_whitespace().count();
        let chars = text.chars().count();
        words.max(chars.div_ceil(4)).max(1)
    }
}

pub fn decide_budget(
    request: BudgetRequest,
    counter: &impl TokenCounter,
) -> Result<BudgetDecision, BudgetError> {
    if request.model.context_limit == 0 {
        return Err(BudgetError::InvalidContextLimit);
    }
    if request.model.max_output_tokens == 0 {
        return Err(BudgetError::InvalidMaxOutputTokens);
    }

    let requested = request
        .requested_max_tokens
        .unwrap_or(request.model.default_max_tokens)
        .min(request.model.max_output_tokens);

    match request.policy {
        BudgetPolicy::StrictReject => strict_decision(request, requested, counter),
        BudgetPolicy::ReserveOutputFirst => reserve_output_first(request, requested, counter),
        BudgetPolicy::TruncateOldest => truncate_oldest(request, requested, counter),
    }
}

fn strict_decision(
    request: BudgetRequest,
    requested: usize,
    counter: &impl TokenCounter,
) -> Result<BudgetDecision, BudgetError> {
    let prompt_tokens = counter.count_prompt(&request.prompt);
    let available = request.model.context_limit.saturating_sub(prompt_tokens);
    let admitted = prompt_tokens + requested <= request.model.context_limit;
    let reason = if admitted {
        "admitted".to_string()
    } else {
        format!(
            "prompt_tokens ({prompt_tokens}) + max_tokens ({requested}) exceeds context_limit ({})",
            request.model.context_limit
        )
    };

    Ok(BudgetDecision {
        admitted,
        reason,
        model: request.model.model,
        prompt_tokens,
        requested_max_tokens: requested,
        admitted_max_tokens: if admitted { requested } else { 0 },
        available_generation_tokens: available,
        truncated_messages: 0,
        prompt: request.prompt,
    })
}

fn reserve_output_first(
    request: BudgetRequest,
    requested: usize,
    counter: &impl TokenCounter,
) -> Result<BudgetDecision, BudgetError> {
    let prompt_tokens = counter.count_prompt(&request.prompt);
    let prompt_budget = request.model.context_limit.saturating_sub(requested);
    let admitted = prompt_tokens <= prompt_budget;
    let reason = if admitted {
        "admitted with requested output reserved".to_string()
    } else {
        format!(
            "prompt_tokens ({prompt_tokens}) exceeds prompt budget after reserving output ({prompt_budget})"
        )
    };

    Ok(BudgetDecision {
        admitted,
        reason,
        model: request.model.model,
        prompt_tokens,
        requested_max_tokens: requested,
        admitted_max_tokens: if admitted { requested } else { 0 },
        available_generation_tokens: request.model.context_limit.saturating_sub(prompt_tokens),
        truncated_messages: 0,
        prompt: request.prompt,
    })
}

fn truncate_oldest(
    request: BudgetRequest,
    requested: usize,
    counter: &impl TokenCounter,
) -> Result<BudgetDecision, BudgetError> {
    let Prompt::Chat(mut messages) = request.prompt else {
        return strict_decision(request, requested, counter);
    };

    let mut truncated = 0;
    while !messages.is_empty()
        && counter.count_prompt(&Prompt::Chat(messages.clone())) + requested
            > request.model.context_limit
    {
        messages.remove(0);
        truncated += 1;
    }

    let prompt = Prompt::Chat(messages);
    let prompt_tokens = counter.count_prompt(&prompt);
    let admitted = prompt_tokens + requested <= request.model.context_limit;
    let reason = if admitted && truncated > 0 {
        format!("admitted after truncating {truncated} oldest messages")
    } else if admitted {
        "admitted".to_string()
    } else {
        "request still exceeds context limit after truncating all messages".to_string()
    };

    Ok(BudgetDecision {
        admitted,
        reason,
        model: request.model.model,
        prompt_tokens,
        requested_max_tokens: requested,
        admitted_max_tokens: if admitted { requested } else { 0 },
        available_generation_tokens: request.model.context_limit.saturating_sub(prompt_tokens),
        truncated_messages: truncated,
        prompt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_model() -> ModelBudget {
        ModelBudget {
            model: "tiny".to_string(),
            context_limit: 40,
            default_max_tokens: 8,
            max_output_tokens: 16,
        }
    }

    #[test]
    fn admits_small_prompt() {
        let request = BudgetRequest {
            prompt: Prompt::Text("hello world".to_string()),
            requested_max_tokens: Some(8),
            policy: BudgetPolicy::StrictReject,
            model: tiny_model(),
        };

        let decision = decide_budget(request, &ApproxTokenCounter).unwrap();

        assert!(decision.admitted);
        assert_eq!(decision.admitted_max_tokens, 8);
    }

    #[test]
    fn rejects_prompt_over_context() {
        let request = BudgetRequest {
            prompt: Prompt::Text("x ".repeat(80)),
            requested_max_tokens: Some(8),
            policy: BudgetPolicy::StrictReject,
            model: tiny_model(),
        };

        let decision = decide_budget(request, &ApproxTokenCounter).unwrap();

        assert!(!decision.admitted);
        assert_eq!(decision.admitted_max_tokens, 0);
    }

    #[test]
    fn truncates_oldest_chat_messages() {
        let request = BudgetRequest {
            prompt: Prompt::Chat(vec![
                ChatMessage {
                    role: "user".to_string(),
                    content: "old ".repeat(40),
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: "new".to_string(),
                },
            ]),
            requested_max_tokens: Some(8),
            policy: BudgetPolicy::TruncateOldest,
            model: tiny_model(),
        };

        let decision = decide_budget(request, &ApproxTokenCounter).unwrap();

        assert!(decision.admitted);
        assert_eq!(decision.truncated_messages, 1);
    }
}
