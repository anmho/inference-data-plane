use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use token_budget_engine::{
    ApproxTokenCounter, BudgetPolicy, BudgetRequest, ChatMessage, ModelBudget, Prompt,
    decide_budget,
};

#[derive(Debug, Parser)]
#[command(about = "Compute prompt and generation token budgets")]
struct Cli {
    input: PathBuf,
    #[arg(long, default_value = "local-vllm")]
    model: String,
    #[arg(long, default_value_t = 4096)]
    context_limit: usize,
    #[arg(long, default_value_t = 256)]
    default_max_tokens: usize,
    #[arg(long, default_value_t = 1024)]
    model_max_output_tokens: usize,
    #[arg(long)]
    max_tokens: Option<usize>,
    #[arg(long, value_enum, default_value = "strict-reject")]
    policy: CliPolicy,
}

#[derive(Debug, Clone, ValueEnum)]
enum CliPolicy {
    StrictReject,
    TruncateOldest,
    ReserveOutputFirst,
}

impl From<CliPolicy> for BudgetPolicy {
    fn from(value: CliPolicy) -> Self {
        match value {
            CliPolicy::StrictReject => Self::StrictReject,
            CliPolicy::TruncateOldest => Self::TruncateOldest,
            CliPolicy::ReserveOutputFirst => Self::ReserveOutputFirst,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let raw = fs::read_to_string(&cli.input)
        .with_context(|| format!("failed to read {}", cli.input.display()))?;
    let prompt = parse_prompt(&raw);
    let request = BudgetRequest {
        prompt,
        requested_max_tokens: cli.max_tokens,
        policy: cli.policy.into(),
        model: ModelBudget {
            model: cli.model,
            context_limit: cli.context_limit,
            default_max_tokens: cli.default_max_tokens,
            max_output_tokens: cli.model_max_output_tokens,
        },
    };

    let decision = decide_budget(request, &ApproxTokenCounter)?;
    println!("{}", serde_json::to_string_pretty(&decision)?);
    Ok(())
}

fn parse_prompt(raw: &str) -> Prompt {
    if let Ok(messages) = serde_json::from_str::<Vec<ChatMessage>>(raw) {
        Prompt::Chat(messages)
    } else {
        Prompt::Text(raw.to_string())
    }
}
