use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use mongreldb_kit::{
    migrate as run_migrations, Column, ColumnType, Database, Migration, MigrationOp, Query, Schema,
    Select, Table,
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
    DropTable {
        name: String,
    },
    AddColumn {
        table: String,
        column: String,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: String,
    },
    AddIndex {
        table: String,
        index: String,
    },
    DropIndex {
        table: String,
        index: String,
    },
    AddUnique {
        table: String,
        constraint: String,
    },
    DropUnique {
        table: String,
        constraint: String,
    },
    AddForeignKey {
        table: String,
        constraint: String,
    },
    DropForeignKey {
        table: String,
        constraint: String,
    },
    AddCheck {
        table: String,
        constraint: String,
    },
    DropCheck {
        table: String,
        constraint: String,
    },
    RawSql(String),
}

impl From<CliMigrationOp> for MigrationOp {
    fn from(op: CliMigrationOp) -> Self {
        match op {
            CliMigrationOp::CreateTable { name, .. } => MigrationOp::CreateTable { name },
            CliMigrationOp::DropTable { name } => MigrationOp::DropTable { name },
            CliMigrationOp::AddColumn { table, column } => MigrationOp::AddColumn { table, column },
            CliMigrationOp::DropColumn { table, column } => {
                MigrationOp::DropColumn { table, column }
            }
            CliMigrationOp::AlterColumn { table, column } => {
                MigrationOp::AlterColumn { table, column }
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
    /// Generate typed row/insert/update definitions for a language.
    Types {
        /// Schema JSON file to read.
        schema: PathBuf,
        /// Target language: `ts`, `rust`, or `python`.
        #[arg(long)]
        lang: String,
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
            GenerateCmd::Types { schema, lang } => cmd_generate_types(&schema, &lang),
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
            println!("[fail] cannot read __kit_schema_migrations: {e}");
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

/// A permissive view of a schema used to check stable table/column IDs without
/// the structural validation `Schema::new` performs. Unknown fields (column
/// types, constraints, ...) are ignored.
#[derive(Debug, Deserialize)]
struct RawSchema {
    #[serde(default)]
    tables: Vec<RawTable>,
}

#[derive(Debug, Deserialize)]
struct RawTable {
    id: u32,
    name: String,
    #[serde(default)]
    columns: Vec<RawColumn>,
}

#[derive(Debug, Deserialize)]
struct RawColumn {
    id: u32,
    name: String,
}

/// Reject duplicate or reused stable table/column IDs with a clear message.
fn validate_stable_ids(raw: &RawSchema) -> Result<()> {
    let mut table_ids: HashMap<u32, String> = HashMap::new();
    let mut table_names: HashMap<String, u32> = HashMap::new();
    for table in &raw.tables {
        if let Some(prev) = table_ids.insert(table.id, table.name.clone()) {
            bail!(
                "duplicate/reused table id {} used by \"{}\" and \"{}\"",
                table.id,
                prev,
                table.name
            );
        }
        if let Some(prev_id) = table_names.insert(table.name.clone(), table.id) {
            bail!(
                "duplicate table name \"{}\" (ids {} and {})",
                table.name,
                prev_id,
                table.id
            );
        }

        let mut col_ids: HashMap<u32, String> = HashMap::new();
        let mut col_names: HashMap<String, u32> = HashMap::new();
        for col in &table.columns {
            if let Some(prev) = col_ids.insert(col.id, col.name.clone()) {
                bail!(
                    "table \"{}\": duplicate/reused column id {} used by \"{}\" and \"{}\"",
                    table.name,
                    col.id,
                    prev,
                    col.name
                );
            }
            if let Some(prev_id) = col_names.insert(col.name.clone(), col.id) {
                bail!(
                    "table \"{}\": duplicate column name \"{}\" (ids {} and {})",
                    table.name,
                    col.name,
                    prev_id,
                    col.id
                );
            }
        }
    }
    Ok(())
}

fn cmd_schema_validate(schema_path: &Path) -> Result<()> {
    let text = fs::read_to_string(schema_path)
        .context(format!("failed to read {}", schema_path.display()))?;
    // Stable-ID checks first so the message names the offending id.
    let raw: RawSchema = serde_json::from_str(&text).context("failed to parse schema JSON")?;
    validate_stable_ids(&raw)?;
    // Full structural validation (primary keys, index/foreign-key references).
    let _schema: Schema = serde_json::from_str(&text).context("schema failed validation")?;
    println!("OK: {}", schema_path.display());
    Ok(())
}

fn cmd_migrate_apply(path: &Path, migrations_path: &Path) -> Result<()> {
    let mut db = Database::open(path).context("failed to open database")?;
    let migrations = read_migrations(migrations_path)?;
    let applied = db
        .applied_migrations()
        .context("failed to read applied migrations")?;
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
    let applied = db
        .applied_migrations()
        .context("failed to read applied migrations")?;
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
    let applied = db
        .applied_migrations()
        .context("failed to read applied migrations")?;
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
    let mut note = |line: String| {
        drift = true;
        println!("{line}");
    };

    // Table add/remove.
    for table in &code.tables {
        if stored.table(&table.name).is_none() {
            note(format!("+ table {}", table.name));
        }
    }
    for table in &stored.tables {
        if code.table(&table.name).is_none() {
            note(format!("- table {}", table.name));
        }
    }

    // Per-table column and constraint diffs for tables present on both sides.
    for table in &code.tables {
        let Some(stored_table) = stored.table(&table.name) else {
            continue;
        };
        diff_columns(&table.name, stored_table, table, &mut note);
        diff_named(
            "unique",
            &table.name,
            stored_table.unique_constraints.iter().map(|u| &u.name),
            table.unique_constraints.iter().map(|u| &u.name),
            &mut note,
        );
        diff_unique_columns(&table.name, stored_table, table, &mut note);
        diff_named(
            "foreign_key",
            &table.name,
            stored_table.foreign_keys.iter().map(|f| &f.name),
            table.foreign_keys.iter().map(|f| &f.name),
            &mut note,
        );
        diff_foreign_keys(&table.name, stored_table, table, &mut note);
        diff_named(
            "index",
            &table.name,
            stored_table.indexes.iter().map(|i| &i.name),
            table.indexes.iter().map(|i| &i.name),
            &mut note,
        );
        diff_indexes(&table.name, stored_table, table, &mut note);
    }

    if !drift {
        println!("no drift");
    }
    Ok(())
}

/// Report added/removed columns, type/nullability/default changes, and stable
/// column-ID reuse between a stored table and the code table.
fn diff_columns(name: &str, stored: &Table, code: &Table, note: &mut impl FnMut(String)) {
    for col in &code.columns {
        if stored.column(&col.name).is_none() {
            note(format!("+ column {name}.{}", col.name));
        }
    }
    for col in &stored.columns {
        if code.column(&col.name).is_none() {
            note(format!("- column {name}.{}", col.name));
        }
    }
    // Property changes for columns present on both sides (matched by name).
    for col in &code.columns {
        if let Some(prev) = stored.column(&col.name) {
            if prev.storage_type != col.storage_type {
                note(format!(
                    "~ column {name}.{} type: {:?} -> {:?}",
                    col.name, prev.storage_type, col.storage_type
                ));
            }
            if prev.nullable != col.nullable {
                note(format!(
                    "~ column {name}.{} nullable: {} -> {}",
                    col.name, prev.nullable, col.nullable
                ));
            }
            if prev.default != col.default {
                note(format!(
                    "~ column {name}.{} default: {:?} -> {:?}",
                    col.name, prev.default, col.default
                ));
            }
        }
    }
    // Stable column-ID reuse: the same id now names a different column.
    let stored_by_id: HashMap<u32, &str> = stored
        .columns
        .iter()
        .map(|c| (c.id, c.name.as_str()))
        .collect();
    for col in &code.columns {
        if let Some(&prev_name) = stored_by_id.get(&col.id) {
            if prev_name != col.name {
                note(format!(
                    "! column id {} on {name} reused: \"{}\" -> \"{}\"",
                    col.id, prev_name, col.name
                ));
            }
        }
    }
}

/// Report added/removed members of a named set (constraints/indexes).
fn diff_named<'a>(
    kind: &str,
    table: &str,
    stored: impl Iterator<Item = &'a String>,
    code: impl Iterator<Item = &'a String>,
    note: &mut impl FnMut(String),
) {
    let stored: Vec<&String> = stored.collect();
    let code: Vec<&String> = code.collect();
    for name in &code {
        if !stored.contains(name) {
            note(format!("+ {kind} {table}.{name}"));
        }
    }
    for name in &stored {
        if !code.contains(name) {
            note(format!("- {kind} {table}.{name}"));
        }
    }
}

fn diff_unique_columns(name: &str, stored: &Table, code: &Table, note: &mut impl FnMut(String)) {
    for uq in &code.unique_constraints {
        if let Some(prev) = stored.unique_constraints.iter().find(|u| u.name == uq.name) {
            if prev.columns != uq.columns {
                note(format!(
                    "~ unique {name}.{} columns: {:?} -> {:?}",
                    uq.name, prev.columns, uq.columns
                ));
            }
        }
    }
}

fn diff_foreign_keys(name: &str, stored: &Table, code: &Table, note: &mut impl FnMut(String)) {
    for fk in &code.foreign_keys {
        if let Some(prev) = stored.foreign_keys.iter().find(|f| f.name == fk.name) {
            if prev.columns != fk.columns
                || prev.references_table != fk.references_table
                || prev.references_columns != fk.references_columns
            {
                note(format!(
                    "~ foreign_key {name}.{} references: {}({}) -> {}({})",
                    fk.name,
                    prev.references_table,
                    prev.references_columns.join(","),
                    fk.references_table,
                    fk.references_columns.join(",")
                ));
            }
            if prev.on_delete != fk.on_delete {
                note(format!(
                    "~ foreign_key {name}.{} on_delete: {:?} -> {:?}",
                    fk.name, prev.on_delete, fk.on_delete
                ));
            }
        }
    }
}

fn diff_indexes(name: &str, stored: &Table, code: &Table, note: &mut impl FnMut(String)) {
    for idx in &code.indexes {
        if let Some(prev) = stored.indexes.iter().find(|i| i.name == idx.name) {
            if prev.columns != idx.columns {
                note(format!(
                    "~ index {name}.{} columns: {:?} -> {:?}",
                    idx.name, prev.columns, idx.columns
                ));
            }
            if prev.unique != idx.unique {
                note(format!(
                    "~ index {name}.{} unique: {} -> {}",
                    idx.name, prev.unique, idx.unique
                ));
            }
        }
    }
}

fn cmd_generate_migration(schema_path: &Path, from: &Path) -> Result<()> {
    let code = read_schema(schema_path)?;
    let db = Database::open(from).context("failed to open database")?;
    let stored = db.schema();
    let applied = db
        .applied_migrations()
        .context("failed to read applied migrations")?;

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
    let json =
        serde_json::to_string_pretty(&vec![migration]).context("failed to serialize migration")?;
    println!("{json}");
    Ok(())
}

/// Convert a `snake_case` / `kebab-case` name to `PascalCase`.
fn pascal_case(name: &str) -> String {
    name.split(['_', '-', ' '])
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut chars = s.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// A column is omitted from the insert type when the kit supplies its value
/// (it has a default or is generated).
fn col_omitted_in_insert(col: &Column) -> bool {
    col.default.is_some() || col.generated
}

fn cmd_generate_types(schema_path: &Path, lang: &str) -> Result<()> {
    let schema = read_schema(schema_path)?;
    let out = match lang {
        "ts" | "typescript" => gen_ts(&schema),
        "rust" | "rs" => gen_rust(&schema),
        "python" | "py" => gen_python(&schema),
        other => bail!("unsupported lang \"{other}\" (expected ts, rust, or python)"),
    };
    print!("{out}");
    Ok(())
}

fn ts_type(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Bool => "boolean",
        ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 => "number",
        ColumnType::Int64 | ColumnType::TimestampNanos => "bigint",
        ColumnType::Float32 | ColumnType::Float64 => "number",
        ColumnType::Text | ColumnType::Date | ColumnType::DateTime => "string",
        ColumnType::Bytes => "Uint8Array",
        ColumnType::Json => "unknown",
    }
}

fn gen_ts(schema: &Schema) -> String {
    let mut out = String::from("// Generated by mongreldb-kit. Do not edit.\n");
    for table in &schema.tables {
        let base = pascal_case(&table.name);

        out.push_str(&format!("\nexport interface {base}Row {{\n"));
        for col in &table.columns {
            let nullable = if col.nullable { " | null" } else { "" };
            out.push_str(&format!(
                "\t{}: {}{nullable};\n",
                col.name,
                ts_type(col.storage_type)
            ));
        }
        out.push_str("}\n");

        out.push_str(&format!("\nexport interface {base}Insert {{\n"));
        for col in &table.columns {
            if col_omitted_in_insert(col) {
                continue;
            }
            let ty = ts_type(col.storage_type);
            if col.nullable {
                out.push_str(&format!("\t{}?: {ty} | null;\n", col.name));
            } else {
                out.push_str(&format!("\t{}: {ty};\n", col.name));
            }
        }
        out.push_str("}\n");

        out.push_str(&format!("\nexport interface {base}Update {{\n"));
        for col in &table.columns {
            let nullable = if col.nullable { " | null" } else { "" };
            out.push_str(&format!(
                "\t{}?: {}{nullable};\n",
                col.name,
                ts_type(col.storage_type)
            ));
        }
        out.push_str("}\n");
    }
    out
}

fn rust_type(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Bool => "bool",
        ColumnType::Int8 => "i8",
        ColumnType::Int16 => "i16",
        ColumnType::Int32 => "i32",
        ColumnType::Int64 | ColumnType::TimestampNanos => "i64",
        ColumnType::Float32 => "f32",
        ColumnType::Float64 => "f64",
        ColumnType::Text | ColumnType::Date | ColumnType::DateTime => "String",
        ColumnType::Bytes => "Vec<u8>",
        ColumnType::Json => "serde_json::Value",
    }
}

fn rust_field_type(col: &Column) -> String {
    let base = rust_type(col.storage_type);
    if col.nullable {
        format!("Option<{base}>")
    } else {
        base.to_string()
    }
}

fn gen_rust(schema: &Schema) -> String {
    let mut out = String::from("// Generated by mongreldb-kit. Do not edit.\n");
    for table in &schema.tables {
        let base = pascal_case(&table.name);

        out.push_str(&format!(
            "\n#[derive(Debug, Clone, PartialEq)]\npub struct {base}Row {{\n"
        ));
        for col in &table.columns {
            out.push_str(&format!(
                "    pub {}: {},\n",
                col.name,
                rust_field_type(col)
            ));
        }
        out.push_str("}\n");

        out.push_str(&format!(
            "\n#[derive(Debug, Clone, PartialEq)]\npub struct {base}Insert {{\n"
        ));
        for col in &table.columns {
            if col_omitted_in_insert(col) {
                continue;
            }
            out.push_str(&format!(
                "    pub {}: {},\n",
                col.name,
                rust_field_type(col)
            ));
        }
        out.push_str("}\n");

        out.push_str(&format!(
            "\n#[derive(Debug, Clone, Default, PartialEq)]\npub struct {base}Update {{\n"
        ));
        for col in &table.columns {
            out.push_str(&format!(
                "    pub {}: Option<{}>,\n",
                col.name,
                rust_type(col.storage_type)
            ));
        }
        out.push_str("}\n");
    }
    out
}

fn python_type(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Bool => "bool",
        ColumnType::Int8
        | ColumnType::Int16
        | ColumnType::Int32
        | ColumnType::Int64
        | ColumnType::TimestampNanos => "int",
        ColumnType::Float32 | ColumnType::Float64 => "float",
        ColumnType::Text | ColumnType::Date | ColumnType::DateTime => "str",
        ColumnType::Bytes => "bytes",
        ColumnType::Json => "Any",
    }
}

fn python_field_type(col: &Column) -> String {
    let base = python_type(col.storage_type);
    if col.nullable {
        format!("Optional[{base}]")
    } else {
        base.to_string()
    }
}

fn gen_python(schema: &Schema) -> String {
    let mut out = String::from("# Generated by mongreldb-kit. Do not edit.\n");
    out.push_str("from __future__ import annotations\n");
    out.push_str("from dataclasses import dataclass\n");
    out.push_str("from typing import Any, Optional\n");

    for table in &schema.tables {
        let base = pascal_case(&table.name);

        out.push_str(&format!("\n\n@dataclass\nclass {base}Row:\n"));
        if table.columns.is_empty() {
            out.push_str("    pass\n");
        } else {
            for col in &table.columns {
                out.push_str(&format!("    {}: {}\n", col.name, python_field_type(col)));
            }
        }

        // Dataclass fields without a default must precede those with one, so emit
        // required (non-nullable) insert fields before omittable ones.
        out.push_str(&format!("\n\n@dataclass\nclass {base}Insert:\n"));
        let insert_cols: Vec<&Column> = table
            .columns
            .iter()
            .filter(|c| !col_omitted_in_insert(c))
            .collect();
        let required: Vec<&Column> = insert_cols
            .iter()
            .copied()
            .filter(|c| !c.nullable)
            .collect();
        let optional: Vec<&Column> = insert_cols.iter().copied().filter(|c| c.nullable).collect();
        if required.is_empty() && optional.is_empty() {
            out.push_str("    pass\n");
        } else {
            for col in &required {
                out.push_str(&format!("    {}: {}\n", col.name, python_field_type(col)));
            }
            for col in &optional {
                out.push_str(&format!(
                    "    {}: {} = None\n",
                    col.name,
                    python_field_type(col)
                ));
            }
        }

        out.push_str(&format!("\n\n@dataclass\nclass {base}Update:\n"));
        if table.columns.is_empty() {
            out.push_str("    pass\n");
        } else {
            for col in &table.columns {
                out.push_str(&format!(
                    "    {}: Optional[{}] = None\n",
                    col.name,
                    python_type(col.storage_type)
                ));
            }
        }
    }
    out
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
        let rows = txn
            .select(&query)
            .context(format!("failed to select from {name}"))?;
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
        let rows = rows
            .as_array()
            .context(format!("{table} value must be an array"))?;
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
