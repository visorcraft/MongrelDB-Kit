use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use mongreldb_kit::{
    migrate as run_migrations, Database, Migration, MigrationOp, Query, Schema, Select,
};
use mongreldb_kit_core::migrations::plan_migrations;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Migration operation shape accepted from JSON files.
///
/// This mirrors [`mongreldb_kit_core::migrations::MigrationOp`] but tolerates
/// extra descriptive fields such as `columns` on `CreateTable` so that
/// migration authors can document the resulting table without the CLI failing
/// to parse.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CliMigrationOp {
    CreateTable {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        columns: Option<Value>,
    },
    DropTable { name: String },
    AddColumn { table: String, column: String },
    DropColumn { table: String, column: String },
    AddIndex { table: String, index: String },
    DropIndex { table: String, index: String },
    AddUnique { table: String, constraint: String },
    DropUnique { table: String, constraint: String },
    AddForeignKey { table: String, constraint: String },
    DropForeignKey { table: String, constraint: String },
    AddCheck { table: String, constraint: String },
    DropCheck { table: String, constraint: String },
    RawSql(String),
}

impl From<CliMigrationOp> for MigrationOp {
    fn from(op: CliMigrationOp) -> Self {
        match op {
            CliMigrationOp::CreateTable { name, .. } => MigrationOp::CreateTable { name },
            CliMigrationOp::DropTable { name } => MigrationOp::DropTable { name },
            CliMigrationOp::AddColumn { table, column } => {
                MigrationOp::AddColumn { table, column }
            }
            CliMigrationOp::DropColumn { table, column } => {
                MigrationOp::DropColumn { table, column }
            }
            CliMigrationOp::AddIndex { table, index } => MigrationOp::AddIndex { table, index },
            CliMigrationOp::DropIndex { table, index } => MigrationOp::DropIndex { table, index },
            CliMigrationOp::AddUnique { table, constraint } => {
                MigrationOp::AddUnique { table, constraint }
            }
            CliMigrationOp::DropUnique { table, constraint } => {
                MigrationOp::DropUnique { table, constraint }
            }
            CliMigrationOp::AddForeignKey { table, constraint } => {
                MigrationOp::AddForeignKey { table, constraint }
            }
            CliMigrationOp::DropForeignKey { table, constraint } => {
                MigrationOp::DropForeignKey { table, constraint }
            }
            CliMigrationOp::AddCheck { table, constraint } => {
                MigrationOp::AddCheck { table, constraint }
            }
            CliMigrationOp::DropCheck { table, constraint } => {
                MigrationOp::DropCheck { table, constraint }
            }
            CliMigrationOp::RawSql(sql) => MigrationOp::RawSql(sql),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CliMigration {
    version: i64,
    name: String,
    ops: Vec<CliMigrationOp>,
}

impl From<CliMigration> for Migration {
    fn from(m: CliMigration) -> Self {
        Self {
            version: m.version,
            name: m.name,
            ops: m.ops.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Parser)]
#[command(name = "mongreldb-kit")]
#[command(about = "MongrelDB Kit command-line interface")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new database directory.
    Init { path: PathBuf },
    /// Open a database and verify internal tables exist.
    Check { path: PathBuf },
    /// Open a database and run an integrity check.
    Doctor { path: PathBuf },
    /// Schema commands.
    #[command(subcommand)]
    Schema(SchemaCmd),
    /// Migration commands.
    #[command(subcommand)]
    Migrate(MigrateCmd),
    /// Compare a code schema to the stored catalog.
    Diff { schema: PathBuf, path: PathBuf },
    /// Generate artifacts from a schema.
    #[command(subcommand)]
    Generate(GenerateCmd),
    /// Fixture commands.
    #[command(subcommand)]
    Fixture(FixtureCmd),
}

#[derive(Subcommand)]
enum SchemaCmd {
    /// Print the stored schema catalog JSON.
    Print { path: PathBuf },
    /// Validate a schema JSON file.
    Validate { schema: PathBuf },
}

#[derive(Subcommand)]
enum MigrateCmd {
    /// Apply pending migrations from a JSON file.
    Apply { path: PathBuf, migrations: PathBuf },
    /// Print applied migration versions.
    Status { path: PathBuf },
    /// Print which migrations would be applied.
    Plan { path: PathBuf, migrations: PathBuf },
    /// Alias for plan.
    DryRun { path: PathBuf, migrations: PathBuf },
}

#[derive(Subcommand)]
enum GenerateCmd {
    /// Generate a migration skeleton for drift.
    Migration {
        schema: PathBuf,
        #[arg(long)]
        from: PathBuf,
    },
}

#[derive(Subcommand)]
enum FixtureCmd {
    /// Dump selected table rows to JSON.
    Create { path: PathBuf, tables: Vec<String> },
    /// Load rows from a JSON fixture into the database.
    Load { path: PathBuf, fixture: PathBuf },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { path } => cmd_init(&path),
        Command::Check { path } => cmd_check(&path),
        Command::Doctor { path } => cmd_doctor(&path),
        Command::Schema(cmd) => match cmd {
            SchemaCmd::Print { path } => cmd_schema_print(&path),
            SchemaCmd::Validate { schema } => cmd_schema_validate(&schema),
        },
        Command::Migrate(cmd) => match cmd {
            MigrateCmd::Apply { path, migrations } => cmd_migrate_apply(&path, &migrations),
            MigrateCmd::Status { path } => cmd_migrate_status(&path),
            MigrateCmd::Plan { path, migrations } => cmd_migrate_plan(&path, &migrations),
            MigrateCmd::DryRun { path, migrations } => cmd_migrate_plan(&path, &migrations),
        },
        Command::Diff { schema, path } => cmd_diff(&schema, &path),
        Command::Generate(cmd) => match cmd {
            GenerateCmd::Migration { schema, from } => cmd_generate_migration(&schema, &from),
        },
        Command::Fixture(cmd) => match cmd {
            FixtureCmd::Create { path, tables } => cmd_fixture_create(&path, &tables),
            FixtureCmd::Load { path, fixture } => cmd_fixture_load(&path, &fixture),
        },
    }
}

fn cmd_init(path: &Path) -> Result<()> {
    let schema = Schema::new(Vec::new()).context("failed to build empty schema")?;
    Database::create(path, schema).context("failed to create database")?;
    println!("initialized {}", path.display());
    Ok(())
}

fn cmd_check(path: &Path) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    db.check_internal_tables()
        .context("internal table check failed")?;
    println!("OK: {}", path.display());
    Ok(())
}

fn cmd_doctor(path: &Path) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    let mut ok = true;

    match db.check_internal_tables() {
        Ok(()) => println!("[ok] internal tables present"),
        Err(e) => {
            ok = false;
            println!("[fail] internal tables: {e}");
        }
    }

    match db.applied_migrations() {
        Ok(migs) => println!("[ok] {} applied migration(s)", migs.len()),
        Err(e) => {
            ok = false;
            println!("[fail] cannot read _migrations: {e}");
        }
    }

    if ok {
        println!("doctor: no problems found");
    } else {
        bail!("doctor found problems");
    }
    Ok(())
}

fn cmd_schema_print(path: &Path) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    let json = serde_json::to_string_pretty(db.schema()).context("failed to serialize schema")?;
    println!("{json}");
    Ok(())
}

fn cmd_schema_validate(schema_path: &Path) -> Result<()> {
    let _schema = read_schema(schema_path)?;
    println!("OK: {}", schema_path.display());
    Ok(())
}

fn cmd_migrate_apply(path: &Path, migrations_path: &Path) -> Result<()> {
    let mut db = Database::open(path).context("failed to open database")?;
    let migrations = read_migrations(migrations_path)?;
    let applied = db.applied_migrations().context("failed to read applied migrations")?;
    let pending = plan_migrations(&applied, &migrations);
    if pending.is_empty() {
        println!("no pending migrations");
        return Ok(());
    }
    run_migrations(&mut db, &migrations).context("migration failed")?;
    for m in pending {
        println!("applied migration {} {}", m.version, m.name);
    }
    Ok(())
}

fn cmd_migrate_status(path: &Path) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    let applied = db.applied_migrations().context("failed to read applied migrations")?;
    if applied.is_empty() {
        println!("no migrations applied");
    } else {
        println!("applied migrations:");
        for m in &applied {
            println!("  {} {}", m.version, m.name);
        }
    }
    Ok(())
}

fn cmd_migrate_plan(path: &Path, migrations_path: &Path) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    let migrations = read_migrations(migrations_path)?;
    let applied = db.applied_migrations().context("failed to read applied migrations")?;
    let pending = plan_migrations(&applied, &migrations);
    if pending.is_empty() {
        println!("no pending migrations");
    } else {
        println!("pending migrations:");
        for m in pending {
            println!("  {} {}", m.version, m.name);
        }
    }
    Ok(())
}

fn cmd_diff(schema_path: &Path, path: &Path) -> Result<()> {
    let code = read_schema(schema_path)?;
    let db = Database::open(path).context("failed to open database")?;
    let stored = db.schema();

    let mut drift = false;

    for table in &code.tables {
        if stored.table(&table.name).is_none() {
            drift = true;
            println!("+ table {}", table.name);
        }
    }
    for table in &stored.tables {
        if code.table(&table.name).is_none() {
            drift = true;
            println!("- table {}", table.name);
        }
    }

    for table in &code.tables {
        if let Some(stored_table) = stored.table(&table.name) {
            for col in &table.columns {
                if stored_table.column(&col.name).is_none() {
                    drift = true;
                    println!("+ column {}.{}", table.name, col.name);
                }
            }
            for col in &stored_table.columns {
                if table.column(&col.name).is_none() {
                    drift = true;
                    println!("- column {}.{}", table.name, col.name);
                }
            }
        }
    }

    if !drift {
        println!("no drift");
    }
    Ok(())
}

fn cmd_generate_migration(schema_path: &Path, from: &Path) -> Result<()> {
    let code = read_schema(schema_path)?;
    let db = Database::open(from).context("failed to open database")?;
    let stored = db.schema();
    let applied = db.applied_migrations().context("failed to read applied migrations")?;

    let next_version = applied.iter().map(|m| m.version).max().unwrap_or(0) + 1;
    let mut ops = Vec::new();

    for table in &code.tables {
        if stored.table(&table.name).is_none() {
            ops.push(MigrationOp::CreateTable {
                name: table.name.clone(),
            });
        } else if let Some(stored_table) = stored.table(&table.name) {
            for col in &table.columns {
                if stored_table.column(&col.name).is_none() {
                    ops.push(MigrationOp::AddColumn {
                        table: table.name.clone(),
                        column: col.name.clone(),
                    });
                }
            }
        }
    }

    let migration = Migration {
        version: next_version,
        name: "generated".into(),
        ops,
    };
    let json = serde_json::to_string_pretty(&vec![migration])
        .context("failed to serialize migration")?;
    println!("{json}");
    Ok(())
}

fn cmd_fixture_create(path: &Path, tables: &[String]) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    let mut out: Map<String, Value> = Map::new();

    let txn = db.begin().context("failed to begin transaction")?;
    for name in tables {
        if db.table(name).is_none() {
            bail!("table {name} not found in schema");
        }
        let query = Query::Select(Select {
            table: name.clone(),
            columns: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
        let rows = txn.select(&query).context(format!("failed to select from {name}"))?;
        let values: Vec<Value> = rows.into_iter().map(|r| Value::Object(r.values)).collect();
        out.insert(name.clone(), Value::Array(values));
    }
    txn.commit().context("failed to commit transaction")?;

    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn cmd_fixture_load(path: &Path, fixture_path: &Path) -> Result<()> {
    let db = Database::open(path).context("failed to open database")?;
    let fixture: Map<String, Value> =
        serde_json::from_reader(fs::File::open(fixture_path)?).context("failed to read fixture")?;

    let mut txn = db.begin().context("failed to begin transaction")?;
    for (table, rows) in fixture {
        if db.table(&table).is_none() {
            bail!("table {table} not found in schema");
        }
        let rows = rows.as_array().context(format!("{table} value must be an array"))?;
        for row in rows {
            let values = row
                .as_object()
                .context(format!("{table} rows must be objects"))?
                .clone();
            txn.insert(&table, values)
                .context(format!("failed to insert into {table}"))?;
        }
    }
    txn.commit().context("failed to commit transaction")?;
    println!("loaded {}", fixture_path.display());
    Ok(())
}

fn read_schema(path: &Path) -> Result<Schema> {
    let text = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    serde_json::from_str(&text).context("failed to parse schema JSON")
}

fn read_migrations(path: &Path) -> Result<Vec<Migration>> {
    let text = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    let cli_migrations: Vec<CliMigration> =
        serde_json::from_str(&text).context("failed to parse migrations JSON")?;
    Ok(cli_migrations.into_iter().map(Into::into).collect())
}
