use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "traceowl-diff",
    version,
    about = "Compare baseline vs candidate retrieval events"
)]
pub struct Args {
    /// Path(s) to baseline event JSONL file(s)
    #[arg(long, num_args = 1..)]
    pub baseline: Vec<PathBuf>,

    /// Path(s) to candidate event JSONL file(s)
    #[arg(long, num_args = 1..)]
    pub candidate: Vec<PathBuf>,

    /// Path to output diff JSONL file
    #[arg(long)]
    pub output: PathBuf,
}
