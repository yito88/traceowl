use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "traceowl-diff", about = "Compare baseline vs candidate retrieval events")]
pub struct Args {
    /// Path to baseline event JSONL file
    #[arg(long)]
    pub baseline: PathBuf,

    /// Path to candidate event JSONL file
    #[arg(long)]
    pub candidate: PathBuf,

    /// Path to output diff JSONL file
    #[arg(long)]
    pub output: PathBuf,
}
