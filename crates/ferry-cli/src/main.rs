use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use tracing_subscriber::EnvFilter;

use ferry_core::config::{DestinationConfig, FerryConfig, FileFormat, SourceConfig, SyncConfig};
use ferry_core::engine::{Engine, RunOptions, SyncResult};
use ferry_core::error::FerryError;
use ferry_core::state::DuckDbStateStore;
use ferry_core::traits::{Destination, Source, StateStore};
use ferry_destinations::FileDestination;
use ferry_sources::duckdb::DuckDbSource;

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "ferry", version, about = "Rust-native reverse ETL")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scaffold a new project
    Init,
    /// Run syncs
    Run(RunArgs),
    /// Validate configs and test connections
    Validate,
    /// Preview what CDC would detect
    Diff(DiffArgs),
    /// Show last run results
    Status,
    /// Dead letter queue management
    Dlq(DlqArgs),
    /// List available source connectors
    Sources,
    /// List available destination connectors
    Destinations,
}

#[derive(Parser)]
struct RunArgs {
    /// Run specific sync by name or tag:<tag>
    #[arg(long)]
    select: Option<String>,
    /// Full refresh — bypass CDC
    #[arg(long)]
    full_refresh: bool,
    /// Preview without writing
    #[arg(long)]
    dry_run: bool,
    /// Include DLQ rows in this run
    #[arg(long)]
    retry_dead: bool,
    /// Structured output for CI
    #[arg(long, value_enum, default_value = "table")]
    output: OutputFormat,
}

#[derive(Parser)]
struct DiffArgs {
    /// Sync name to diff
    #[arg(long)]
    select: String,
}

#[derive(Parser)]
struct DlqArgs {
    #[command(subcommand)]
    action: DlqAction,
}

#[derive(Subcommand)]
enum DlqAction {
    /// List dead rows
    List {
        /// Filter by sync name
        #[arg(long)]
        sync: Option<String>,
    },
    /// Retry dead rows
    Retry {
        /// Filter by sync name
        #[arg(long)]
        sync: Option<String>,
    },
    /// Purge old dead rows
    Purge {
        /// Purge rows older than this duration (e.g. "7d", "30d", "24h")
        #[arg(long)]
        older_than: String,
    },
}

#[derive(ValueEnum, Clone)]
enum OutputFormat {
    Table,
    Json,
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Init => cmd_init(Path::new(".")),
        Commands::Run(args) => cmd_run(args).await,
        Commands::Validate => cmd_validate().await,
        Commands::Diff(args) => cmd_diff(args).await,
        Commands::Status => cmd_status().await,
        Commands::Dlq(args) => cmd_dlq(args).await,
        Commands::Sources => cmd_sources(),
        Commands::Destinations => cmd_destinations(),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

/// `ferry init` — scaffold a new project
fn cmd_init(project_dir: &Path) -> Result<(), FerryError> {
    // Create directories
    let syncs_dir = project_dir.join("syncs");
    let ferry_dir = project_dir.join(".ferry");
    let output_dir = project_dir.join("output");

    create_dir_if_not_exists(&syncs_dir)?;
    create_dir_if_not_exists(&ferry_dir)?;
    create_dir_if_not_exists(&output_dir)?;

    // Write ferry.yml
    let ferry_yml_path = project_dir.join("ferry.yml");
    if !ferry_yml_path.exists() {
        std::fs::write(
            &ferry_yml_path,
            r#"name: my-ferry-project
version: 1

source:
  type: duckdb
  path: ./data/source.duckdb

state:
  backend: duckdb
  path: ./.ferry/state.duckdb

defaults:
  mode: incremental
  delivery:
    batch_size: 1000
    retry:
      max_attempts: 3
      backoff: exponential
      initial_delay_secs: 30
    allow_redelivery: false
    dead_letter:
      max_age_secs: 604800
"#,
        )
        .map_err(|e| FerryError::Config(format!("Failed to write ferry.yml: {e}")))?;
        println!("Created ferry.yml");
    } else {
        println!("ferry.yml already exists, skipping");
    }

    // Write example sync
    let example_sync_path = syncs_dir.join("example_sync.yml");
    if !example_sync_path.exists() {
        std::fs::write(
            &example_sync_path,
            r#"name: example_sync
description: "Example sync — exports users to a CSV file"
tags: [example]

model:
  sql: SELECT 1 as id, 'Alice' as name, 'alice@example.com' as email

destination:
  type: file
  output_dir: ./output
  format: csv

sync:
  mode: incremental
  cdc:
    method: hash
    hash_columns:
      - id
      - name
      - email
  delivery:
    batch_size: 1000
    retry:
      max_attempts: 3
      backoff: exponential
      initial_delay_secs: 30
"#,
        )
        .map_err(|e| FerryError::Config(format!("Failed to write example sync: {e}")))?;
        println!("Created syncs/example_sync.yml");
    } else {
        println!("syncs/example_sync.yml already exists, skipping");
    }

    // Write secrets.toml template
    let secrets_path = project_dir.join("secrets.toml");
    if !secrets_path.exists() {
        std::fs::write(
            &secrets_path,
            r#"# Ferry secrets file
# Store sensitive values here instead of in ferry.yml or sync YAML files.
# Use ${VAR} syntax in YAML files to reference these values.
#
# Example:
# [source.duckdb]
# path = "/path/to/secure/database.duckdb"
#
# [destination.rest]
# api_key = "your-api-key-here"
"#,
        )
        .map_err(|e| FerryError::Config(format!("Failed to write secrets.toml: {e}")))?;

        // Set permissions to 600 (owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&secrets_path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| {
                    FerryError::Config(format!("Failed to set secrets.toml permissions: {e}"))
                })?;
        }
        println!("Created secrets.toml (permissions set to 600)");
    } else {
        println!("secrets.toml already exists, skipping");
    }

    // Update .gitignore
    let gitignore_path = project_dir.join(".gitignore");
    let mut gitignore_content = if gitignore_path.exists() {
        std::fs::read_to_string(&gitignore_path)
            .map_err(|e| FerryError::Config(format!("Failed to read .gitignore: {e}")))?
    } else {
        String::new()
    };

    let entries_to_add = ["secrets.toml", ".ferry/"];
    let mut modified = false;
    for entry in &entries_to_add {
        if !gitignore_content.contains(entry) {
            gitignore_content.push_str(entry);
            gitignore_content.push('\n');
            modified = true;
        }
    }

    if modified {
        std::fs::write(&gitignore_path, gitignore_content)
            .map_err(|e| FerryError::Config(format!("Failed to write .gitignore: {e}")))?;
        println!("Updated .gitignore");
    }

    println!();
    println!("Ferry project initialized successfully!");
    println!();
    println!("Next steps:");
    println!("  1. Edit ferry.yml to configure your source database");
    println!("  2. Add sync definitions in syncs/ directory");
    println!("  3. Run `ferry validate` to check your configuration");
    println!("  4. Run `ferry run` to execute syncs");

    Ok(())
}

/// `ferry run` — execute syncs
async fn cmd_run(args: RunArgs) -> Result<(), FerryError> {
    let project_dir = Path::new(".");
    let config = FerryConfig::load(project_dir)?;
    let syncs_dir = project_dir.join("syncs");
    let all_syncs = SyncConfig::load_all(&syncs_dir)?;

    // Extract state path before config is moved into Engine
    let state_path = config
        .state
        .path
        .clone()
        .unwrap_or_else(|| ".ferry/state.duckdb".to_string());

    // Filter by --select
    let selected_syncs = filter_syncs(&all_syncs, args.select.as_deref())?;

    if selected_syncs.is_empty() {
        eprintln!("No syncs matched the selection criteria");
        return Ok(());
    }

    let engine = Engine::new(config)?;
    let mut results: Vec<SyncResult> = Vec::new();

    for sync_config in &selected_syncs {
        // Create source
        let source = create_source_from_config(project_dir, sync_config)?;

        // Create destination
        let destination = create_destination_from_config(project_dir, sync_config)?;

        let options = RunOptions {
            sync_names: Some(vec![sync_config.name.clone()]),
            full_refresh: args.full_refresh,
            dry_run: args.dry_run,
            retry_dead: args.retry_dead,
        };

        // If --retry-dead, retry dead rows before running
        if args.retry_dead && !args.dry_run {
            let state = DuckDbStateStore::new(project_dir.join(&state_path).as_path())?;
            let retried = state.retry_dead_rows(&sync_config.name, None).await?;
            if retried > 0 {
                println!(
                    "Retried {} dead rows for sync '{}'",
                    retried, sync_config.name
                );
            }
        }

        let result = engine
            .run_sync(sync_config, source.as_ref(), destination.as_ref(), &options)
            .await?;
        results.push(result);
    }

    // Print results
    match args.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&results)
                .map_err(|e| FerryError::Config(format!("Failed to serialize results: {e}")))?;
            println!("{json}");
        }
        OutputFormat::Table => {
            print_sync_results_table(&results);
        }
    }

    Ok(())
}

/// `ferry validate` — validate configs and test connections
async fn cmd_validate() -> Result<(), FerryError> {
    let project_dir = Path::new(".");
    let config = FerryConfig::load(project_dir)?;
    let syncs_dir = project_dir.join("syncs");
    let all_syncs = SyncConfig::load_all(&syncs_dir)?;

    let engine = Engine::new(config)?;
    let validation_errors = engine.validate(&syncs_dir).await?;

    let mut has_errors = false;

    // Test source connections
    for sync_config in &all_syncs {
        match create_source_from_config(project_dir, sync_config) {
            Ok(source) => match source.check_connection().await {
                Ok(()) => {
                    println!("✓ Source connection OK for sync '{}'", sync_config.name);
                }
                Err(e) => {
                    eprintln!(
                        "✗ Source connection FAILED for sync '{}': {e}",
                        sync_config.name
                    );
                    has_errors = true;
                }
            },
            Err(e) => {
                eprintln!(
                    "✗ Failed to create source for sync '{}': {e}",
                    sync_config.name
                );
                has_errors = true;
            }
        }

        // Test destination connections
        match create_destination_from_config(project_dir, sync_config) {
            Ok(dest) => match dest.check_connection().await {
                Ok(()) => {
                    println!(
                        "✓ Destination connection OK for sync '{}'",
                        sync_config.name
                    );
                }
                Err(e) => {
                    eprintln!(
                        "✗ Destination connection FAILED for sync '{}': {e}",
                        sync_config.name
                    );
                    has_errors = true;
                }
            },
            Err(e) => {
                eprintln!(
                    "✗ Failed to create destination for sync '{}': {e}",
                    sync_config.name
                );
                has_errors = true;
            }
        }
    }

    // Print validation errors
    for err in &validation_errors {
        eprintln!("✗ Validation error: {err}");
        has_errors = true;
    }

    if has_errors {
        eprintln!("\nValidation completed with errors.");
        return Err(FerryError::Validation("Some checks failed".to_string()));
    }

    println!("\n✓ All validations passed!");
    Ok(())
}

/// `ferry diff --select <name>` — preview CDC diff
async fn cmd_diff(args: DiffArgs) -> Result<(), FerryError> {
    let project_dir = Path::new(".");
    let config = FerryConfig::load(project_dir)?;
    let syncs_dir = project_dir.join("syncs");
    let all_syncs = SyncConfig::load_all(&syncs_dir)?;

    let sync_config = all_syncs
        .iter()
        .find(|s| s.name == args.select)
        .ok_or_else(|| {
            FerryError::Config(format!(
                "Sync '{}' not found in syncs/ directory",
                args.select
            ))
        })?;

    let source = create_source_from_config(project_dir, sync_config)?;
    let engine = Engine::new(config)?;

    let preview = engine
        .diff(&args.select, source.as_ref(), sync_config)
        .await?;

    println!("Diff preview for sync '{}':", preview.sync_name);
    println!("  Total rows:  {}", preview.total_rows);
    println!("  Added:       {}", preview.added);
    println!("  Changed:     {}", preview.changed);
    println!("  Removed:     {}", preview.removed);

    Ok(())
}

/// `ferry status` — show run history
async fn cmd_status() -> Result<(), FerryError> {
    let project_dir = Path::new(".");
    let config = FerryConfig::load(project_dir)?;
    let syncs_dir = project_dir.join("syncs");
    let all_syncs = SyncConfig::load_all(&syncs_dir)?;

    let state_path = config
        .state
        .path
        .as_deref()
        .unwrap_or(".ferry/state.duckdb");
    let state = DuckDbStateStore::new(project_dir.join(state_path).as_path())?;

    println!(
        "{:<20} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<20}",
        "Sync", "Extracted", "Synced", "Failed", "Pending", "Dead", "Mode", "Status"
    );
    println!("{}", "-".repeat(110));

    for sync_config in &all_syncs {
        let runs = state.get_runs(&sync_config.name, 5).await?;
        if runs.is_empty() {
            println!(
                "{:<20} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<20}",
                sync_config.name, "-", "-", "-", "-", "-", "-", "no runs yet"
            );
        } else {
            for run in &runs {
                let status = if run.status == "completed" {
                    "completed"
                } else {
                    "running"
                };
                println!(
                    "{:<20} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<20}",
                    sync_config.name,
                    run.rows_extracted,
                    run.rows_synced,
                    run.rows_failed,
                    0, // pending not stored in sync_runs
                    run.rows_dead,
                    run.mode,
                    status,
                );
            }
        }
    }

    Ok(())
}

/// `ferry dlq` — dead letter queue management
async fn cmd_dlq(args: DlqArgs) -> Result<(), FerryError> {
    let project_dir = Path::new(".");
    let config = FerryConfig::load(project_dir)?;
    let syncs_dir = project_dir.join("syncs");
    let all_syncs = SyncConfig::load_all(&syncs_dir)?;

    let state_path = config
        .state
        .path
        .as_deref()
        .unwrap_or(".ferry/state.duckdb");
    let state = DuckDbStateStore::new(project_dir.join(state_path).as_path())?;

    match args.action {
        DlqAction::List { sync } => {
            let sync_names: Vec<&str> = if let Some(ref name) = sync {
                vec![name.as_str()]
            } else {
                all_syncs.iter().map(|s| s.name.as_str()).collect()
            };

            let mut total_dead = 0usize;
            for sync_name in &sync_names {
                let dead_rows = state.get_dead_rows(sync_name).await?;
                if dead_rows.is_empty() {
                    continue;
                }
                println!("Dead rows for sync '{}':", sync_name);
                println!(
                    "  {:<20} {:<10} {:<30} {:<20}",
                    "Primary Key", "Attempts", "Last Error", "Last Attempt"
                );
                for row in &dead_rows {
                    let last_attempt = row
                        .last_attempt_at
                        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "-".to_string());
                    let error = row
                        .last_error
                        .as_deref()
                        .unwrap_or("-")
                        .chars()
                        .take(28)
                        .collect::<String>();
                    println!(
                        "  {:<20} {:<10} {:<30} {:<20}",
                        row.primary_key, row.attempts, error, last_attempt
                    );
                }
                total_dead += dead_rows.len();
            }

            if total_dead == 0 {
                println!("No dead rows found.");
            } else {
                println!("\nTotal dead rows: {total_dead}");
            }
        }
        DlqAction::Retry { sync } => {
            let sync_names: Vec<&str> = if let Some(ref name) = sync {
                vec![name.as_str()]
            } else {
                all_syncs.iter().map(|s| s.name.as_str()).collect()
            };

            let mut total_retried = 0usize;
            for sync_name in &sync_names {
                let count = state.retry_dead_rows(sync_name, None).await?;
                if count > 0 {
                    println!("Retried {count} dead rows for sync '{sync_name}'");
                }
                total_retried += count;
            }

            if total_retried == 0 {
                println!("No dead rows to retry.");
            } else {
                println!("\nTotal retried: {total_retried}");
            }
        }
        DlqAction::Purge { older_than } => {
            let duration = parse_duration(&older_than)?;
            let sync_names: Vec<&str> = all_syncs.iter().map(|s| s.name.as_str()).collect();

            let mut total_purged = 0usize;
            for sync_name in &sync_names {
                let count = state.purge_dead_rows(sync_name, duration).await?;
                if count > 0 {
                    println!("Purged {count} dead rows for sync '{sync_name}'");
                }
                total_purged += count;
            }

            if total_purged == 0 {
                println!("No dead rows to purge.");
            } else {
                println!("\nTotal purged: {total_purged}");
            }
        }
    }

    Ok(())
}

/// `ferry sources` — list available source connectors
fn cmd_sources() -> Result<(), FerryError> {
    println!("Available source connectors:");
    println!();
    println!("  {:<15} {:<40}", "Name", "Description");
    println!("  {:<15} {:<40}", "----", "-----------");
    println!("  {:<15} {:<40}", "duckdb", "DuckDB database (MVP)");
    println!();
    println!("Use `type: duckdb` in ferry.yml to configure.");
    Ok(())
}

/// `ferry destinations` — list available destination connectors
fn cmd_destinations() -> Result<(), FerryError> {
    println!("Available destination connectors:");
    println!();
    println!("  {:<15} {:<40}", "Name", "Description");
    println!("  {:<15} {:<40}", "----", "-----------");
    println!("  {:<15} {:<40}", "file", "CSV/JSON file output (MVP)");
    println!("  {:<15} {:<40}", "rest", "Generic REST API");
    println!("  {:<15} {:<40}", "braze", "Braze (planned)");
    println!("  {:<15} {:<40}", "slack", "Slack webhook (planned)");
    println!();
    println!("Use `type: <name>` in sync YAML to configure.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Factory functions
// ---------------------------------------------------------------------------

/// Create a source connector from a sync config.
fn create_source_from_config(
    project_dir: &Path,
    _sync_config: &SyncConfig,
) -> Result<Box<dyn Source>, FerryError> {
    // For now, we use the project-level source config from ferry.yml
    // In Phase 2, sync-level source overrides will be supported.
    let config = FerryConfig::load(project_dir)?;
    match &config.source {
        SourceConfig::DuckDB { path, query: _ } => {
            let resolved_path = if Path::new(path).is_relative() {
                project_dir.join(path)
            } else {
                PathBuf::from(path)
            };
            let source = DuckDbSource::new(
                resolved_path
                    .to_str()
                    .ok_or_else(|| FerryError::Config("Invalid source path".to_string()))?,
            )?;
            Ok(Box::new(source))
        }
    }
}

/// Create a destination connector from a sync config.
fn create_destination_from_config(
    project_dir: &Path,
    sync_config: &SyncConfig,
) -> Result<Box<dyn Destination>, FerryError> {
    match &sync_config.destination {
        DestinationConfig::File { output_dir, format } => {
            let resolved_dir = if Path::new(output_dir).is_relative() {
                project_dir.join(output_dir)
            } else {
                PathBuf::from(output_dir)
            };
            let file_format = match format {
                Some(f) => match f {
                    FileFormat::Csv => ferry_destinations::FileFormat::Csv,
                    FileFormat::Json => ferry_destinations::FileFormat::Json,
                },
                None => ferry_destinations::FileFormat::Csv,
            };
            let dest = FileDestination::new(&resolved_dir, file_format, &sync_config.name);
            Ok(Box::new(dest))
        }
        DestinationConfig::Rest { .. } => Err(FerryError::Config(
            "REST destination not yet implemented in Phase 1".to_string(),
        )),
        DestinationConfig::Braze { .. } => Err(FerryError::Config(
            "Braze destination not yet implemented in Phase 1".to_string(),
        )),
        DestinationConfig::Slack { .. } => Err(FerryError::Config(
            "Slack destination not yet implemented in Phase 1".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Create a directory if it doesn't exist.
fn create_dir_if_not_exists(path: &Path) -> Result<(), FerryError> {
    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|e| {
            FerryError::Config(format!(
                "Failed to create directory '{}': {e}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

/// Filter syncs by --select argument (name or tag:<tag>).
fn filter_syncs<'a>(
    syncs: &'a [SyncConfig],
    select: Option<&str>,
) -> Result<Vec<&'a SyncConfig>, FerryError> {
    let Some(select) = select else {
        return Ok(syncs.iter().collect());
    };

    if let Some(tag) = select.strip_prefix("tag:") {
        let tag = tag.to_string();
        let matched: Vec<&SyncConfig> = syncs
            .iter()
            .filter(|s| s.tags.as_ref().map(|t| t.contains(&tag)).unwrap_or(false))
            .collect();
        if matched.is_empty() {
            return Err(FerryError::Config(format!(
                "No syncs found with tag '{tag}'"
            )));
        }
        return Ok(matched);
    }

    // Filter by name
    let matched: Vec<&SyncConfig> = syncs.iter().filter(|s| s.name == select).collect();
    if matched.is_empty() {
        return Err(FerryError::Config(format!(
            "No sync found with name '{select}'"
        )));
    }
    Ok(matched)
}

/// Parse a duration string like "7d", "30d", "24h" into chrono::Duration.
fn parse_duration(s: &str) -> Result<chrono::Duration, FerryError> {
    let s = s.trim();
    if let Some(days) = s.strip_suffix('d') {
        let num: i64 = days.parse().map_err(|_| {
            FerryError::Config(format!("Invalid duration '{s}': expected number of days"))
        })?;
        Ok(chrono::Duration::days(num))
    } else if let Some(hours) = s.strip_suffix('h') {
        let num: i64 = hours.parse().map_err(|_| {
            FerryError::Config(format!("Invalid duration '{s}': expected number of hours"))
        })?;
        Ok(chrono::Duration::hours(num))
    } else if let Some(minutes) = s.strip_suffix('m') {
        let num: i64 = minutes.parse().map_err(|_| {
            FerryError::Config(format!(
                "Invalid duration '{s}': expected number of minutes"
            ))
        })?;
        Ok(chrono::Duration::minutes(num))
    } else if let Some(secs) = s.strip_suffix('s') {
        let num: i64 = secs.parse().map_err(|_| {
            FerryError::Config(format!(
                "Invalid duration '{s}': expected number of seconds"
            ))
        })?;
        Ok(chrono::Duration::seconds(num))
    } else {
        // Try parsing as seconds
        let num: i64 = s.parse().map_err(|_| {
            FerryError::Config(format!(
                "Invalid duration '{s}'. Use format like '7d', '30d', '24h', '90m', '3600s'"
            ))
        })?;
        Ok(chrono::Duration::seconds(num))
    }
}

/// Print sync results in a table format.
fn print_sync_results_table(results: &[SyncResult]) {
    if results.is_empty() {
        println!("No syncs were run.");
        return;
    }

    println!();
    println!(
        "{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<8}",
        "Sync",
        "Extracted",
        "Synced",
        "Failed",
        "Pending",
        "Retried",
        "Dead",
        "Duration",
        "Dry Run"
    );
    println!("{}", "-".repeat(120));

    for result in results {
        let dry_run_str = if result.dry_run { "yes" } else { "no" };
        println!(
            "{:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10.2}s {:<8}",
            result.sync_name,
            result.rows_extracted,
            result.rows_synced,
            result.rows_failed,
            result.rows_pending,
            result.rows_retried,
            result.rows_dead,
            result.duration_seconds,
            dry_run_str,
        );
    }
    println!();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_creates_project() {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");

        let result = cmd_init(dir.path());
        assert!(result.is_ok(), "init should succeed: {:?}", result);

        // Verify all files created
        assert!(
            dir.path().join("ferry.yml").exists(),
            "ferry.yml should exist"
        );
        assert!(dir.path().join("syncs").exists(), "syncs/ dir should exist");
        assert!(
            dir.path().join(".ferry").exists(),
            ".ferry/ dir should exist"
        );
        assert!(
            dir.path().join("output").exists(),
            "output/ dir should exist"
        );
        assert!(
            dir.path().join("syncs/example_sync.yml").exists(),
            "syncs/example_sync.yml should exist"
        );
        assert!(
            dir.path().join("secrets.toml").exists(),
            "secrets.toml should exist"
        );
    }

    #[test]
    fn test_init_ferry_yml_valid() {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");

        cmd_init(dir.path()).expect("init should succeed");

        // Parse the generated ferry.yml
        let config = FerryConfig::load(dir.path());
        assert!(
            config.is_ok(),
            "Generated ferry.yml should parse: {:?}",
            config.err()
        );
    }

    #[test]
    fn test_init_example_sync_valid() {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");

        cmd_init(dir.path()).expect("init should succeed");

        // Parse the generated example sync
        let sync_path = dir.path().join("syncs/example_sync.yml");
        let config = SyncConfig::load(&sync_path);
        assert!(
            config.is_ok(),
            "Generated example sync should parse: {:?}",
            config.err()
        );

        let config = config.unwrap();
        assert_eq!(config.name, "example_sync");
        assert_eq!(config.tags, Some(vec!["example".to_string()]));
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("7d").unwrap(), chrono::Duration::days(7));
        assert_eq!(parse_duration("24h").unwrap(), chrono::Duration::hours(24));
        assert_eq!(
            parse_duration("90m").unwrap(),
            chrono::Duration::minutes(90)
        );
        assert_eq!(
            parse_duration("3600s").unwrap(),
            chrono::Duration::seconds(3600)
        );
        assert_eq!(parse_duration("30").unwrap(), chrono::Duration::seconds(30));
        assert!(parse_duration("invalid").is_err());
    }

    #[test]
    fn test_filter_syncs_by_name() {
        let syncs = vec![
            SyncConfig {
                name: "users".to_string(),
                ..create_test_sync_config("users")
            },
            SyncConfig {
                name: "orders".to_string(),
                ..create_test_sync_config("orders")
            },
        ];

        let filtered = filter_syncs(&syncs, Some("users")).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "users");
    }

    #[test]
    fn test_filter_syncs_by_tag() {
        let syncs = vec![
            SyncConfig {
                name: "users".to_string(),
                tags: Some(vec!["production".to_string()]),
                ..create_test_sync_config("users")
            },
            SyncConfig {
                name: "orders".to_string(),
                tags: Some(vec!["staging".to_string()]),
                ..create_test_sync_config("orders")
            },
        ];

        let filtered = filter_syncs(&syncs, Some("tag:production")).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "users");
    }

    #[test]
    fn test_filter_syncs_all() {
        let syncs = vec![
            create_test_sync_config("users"),
            create_test_sync_config("orders"),
        ];

        let filtered = filter_syncs(&syncs, None).unwrap();
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_filter_syncs_no_match() {
        let syncs = vec![create_test_sync_config("users")];
        let result = filter_syncs(&syncs, Some("nonexistent"));
        assert!(result.is_err());
    }

    fn create_test_sync_config(name: &str) -> SyncConfig {
        use ferry_core::config::*;
        SyncConfig {
            name: name.to_string(),
            description: Some("test".to_string()),
            tags: None,
            model: ModelConfig::Sql {
                sql: "SELECT 1".to_string(),
            },
            destination: DestinationConfig::File {
                output_dir: "./output".to_string(),
                format: Some(FileFormat::Csv),
            },
            sync: SyncSettings {
                mode: SyncMode::Incremental,
                cursor_field: None,
                cdc: Some(CdcConfig {
                    method: CdcMethod::Hash,
                    hash_columns: None,
                }),
                delivery: Some(DeliveryConfig {
                    batch_size: 100,
                    retry: Some(RetryConfig {
                        max_attempts: 3,
                        backoff: BackoffStrategy::Exponential,
                        initial_delay_secs: 5,
                        max_delay_secs: 300,
                    }),
                    on_reject: None,
                    dead_letter: None,
                    allow_redelivery: false,
                }),
                full_refresh: None,
            },
            tests: None,
        }
    }
}
