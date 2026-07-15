use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use mongreldb_kit::{
    migrate as run_migrations, AggFunc, Aggregate, AggregateQuery, Column, ColumnType, Database,
    Direction, Expr, Literal, Migration, MigrationOp, OnConflict, OrderBy, Permission, Query,
    QueryId, Schema, Select, SqlOptions, Table,
};
use mongreldb_kit_core::migrations::plan_migrations;
use mongreldb_kit_core::{ProcedureSpec, TriggerSpec, ViewSpec, VirtualTableSpec};
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
    CreateProcedure {
        name: String,
        procedure: Value,
    },
    ReplaceProcedure {
        name: String,
        procedure: Value,
    },
    DropProcedure {
        name: String,
    },
    CreateTrigger {
        name: String,
        trigger: Value,
    },
    ReplaceTrigger {
        name: String,
        trigger: Value,
    },
    DropTrigger {
        name: String,
    },
    CreateVirtualTable {
        table: VirtualTableSpec,
    },
    DropVirtualTable {
        name: String,
    },
    CreateView {
        name: String,
        view: ViewSpec,
    },
    ReplaceView {
        name: String,
        view: ViewSpec,
    },
    DropView {
        name: String,
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
            CliMigrationOp::CreateProcedure { name, procedure } => MigrationOp::CreateProcedure {
                name,
                procedure: ProcedureSpec::new(procedure),
            },
            CliMigrationOp::ReplaceProcedure { name, procedure } => MigrationOp::ReplaceProcedure {
                name,
                procedure: ProcedureSpec::new(procedure),
            },
            CliMigrationOp::DropProcedure { name } => MigrationOp::DropProcedure { name },
            CliMigrationOp::CreateTrigger { name, trigger } => MigrationOp::CreateTrigger {
                name,
                trigger: TriggerSpec::new(trigger),
            },
            CliMigrationOp::ReplaceTrigger { name, trigger } => MigrationOp::ReplaceTrigger {
                name,
                trigger: TriggerSpec::new(trigger),
            },
            CliMigrationOp::DropTrigger { name } => MigrationOp::DropTrigger { name },
            CliMigrationOp::CreateVirtualTable { table } => {
                MigrationOp::CreateVirtualTable { table }
            }
            CliMigrationOp::DropVirtualTable { name } => MigrationOp::DropVirtualTable { name },
            CliMigrationOp::CreateView { name, view } => MigrationOp::CreateView { name, view },
            CliMigrationOp::ReplaceView { name, view } => MigrationOp::ReplaceView { name, view },
            CliMigrationOp::DropView { name } => MigrationOp::DropView { name },
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
    /// Username for credentialed databases (require_auth = true).
    #[arg(long, global = true, env = "MONGREL_USER")]
    user: Option<String>,

    /// Password for credentialed databases. Used with --user.
    #[arg(long, global = true, env = "MONGREL_PASSWORD")]
    password: Option<String>,

    /// Read the password from stdin (one line). Requires --user.
    #[arg(long, global = true)]
    password_stdin: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new database directory.
    Init {
        path: PathBuf,
        /// Create with require_auth = true and an initial admin user.
        #[arg(long)]
        require_auth: bool,
        /// Admin username (requires --require-auth).
        #[arg(long, requires = "require_auth")]
        admin_user: Option<String>,
        /// Admin password (requires --require-auth). Also via MONGREL_PASSWORD.
        #[arg(long, requires = "require_auth", env = "MONGREL_PASSWORD")]
        admin_password: Option<String>,
    },
    /// Open a database and verify internal tables exist.
    Check { path: PathBuf },
    /// Open a database and run an integrity check.
    Doctor { path: PathBuf },
    /// Remove all rows from a table.
    Truncate { path: PathBuf, table: String },
    /// Point-read a single row by primary key.
    Get {
        path: PathBuf,
        table: String,
        pk: String,
    },
    /// Query rows with an optional friendly filter, ordering, and projection.
    Query {
        path: PathBuf,
        table: String,
        /// Friendly filter JSON, e.g. '{"amount":{"gte":100},"region":"east"}'.
        #[arg(long)]
        filter: Option<String>,
        /// Order key(s), comma-separated, `+col` asc / `-col` desc.
        #[arg(long)]
        order: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        offset: Option<usize>,
        /// Projected columns (comma-separated); default is all columns.
        #[arg(long, value_delimiter = ',')]
        columns: Option<Vec<String>>,
        /// Drop duplicate rows.
        #[arg(long)]
        distinct: bool,
    },
    /// Insert one row (JSON object) and print it with defaults applied.
    Insert {
        path: PathBuf,
        table: String,
        row: String,
    },
    /// Update a row by primary key with a JSON patch object.
    Update {
        path: PathBuf,
        table: String,
        pk: String,
        patch: String,
    },
    /// Delete a row by primary key.
    Delete {
        path: PathBuf,
        table: String,
        pk: String,
    },
    /// Insert a row, or update it on a primary-key conflict with `--update`.
    Upsert {
        path: PathBuf,
        table: String,
        row: String,
        /// On conflict, update with the provided row instead of doing nothing.
        #[arg(long)]
        update: bool,
    },
    /// Count rows, optionally matching a friendly filter.
    Count {
        path: PathBuf,
        table: String,
        #[arg(long)]
        filter: Option<String>,
    },
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
    /// Stored procedure commands.
    #[command(subcommand)]
    Procedure(ProcedureCmd),
    /// Compact all tables — merge sorted runs into one for flat query latency.
    Compact { path: PathBuf },
    /// Rebuild index statistics for every table (engine ANALYZE).
    Analyze { path: PathBuf },
    /// Reclaim space: compact every table, then gc (engine VACUUM).
    Vacuum { path: PathBuf },
    /// Rename a live table (engine + kit schema catalog).
    RenameTable {
        path: PathBuf,
        from: String,
        to: String,
    },
    /// Run a SQL statement (read returns rows as JSON; DDL/DML returns []).
    Sql {
        path: PathBuf,
        statement: String,
        /// Server-side SQL deadline in milliseconds.
        #[arg(long)]
        timeout_ms: Option<u64>,
        /// Optional 32-hex-character query ID for logs and cancellation.
        #[arg(long)]
        query_id: Option<String>,
    },
    /// SQL view commands.
    #[command(subcommand)]
    View(ViewCmd),
    /// Secondary index commands.
    #[command(subcommand)]
    Index(IndexCmd),
    /// User and credential commands.
    #[command(subcommand)]
    User(UserCmd),
    /// Role and permission commands.
    #[command(subcommand)]
    Role(RoleCmd),
    /// Auth enforcement commands (enable/disable require_auth).
    #[command(subcommand)]
    Auth(AuthCmd),
}

#[derive(Subcommand)]
enum ViewCmd {
    /// Create a view from a JSON spec `{"name": ..., "sql": "SELECT ..."}`.
    Create { path: PathBuf, view: PathBuf },
    /// Drop a view by name (idempotent).
    Drop { path: PathBuf, name: String },
}

#[derive(Subcommand)]
enum IndexCmd {
    /// Create a secondary index on a table column.
    Create {
        path: PathBuf,
        table: String,
        name: String,
        #[arg(long)]
        column: String,
        #[arg(long, default_value = "bitmap")]
        kind: String,
    },
    /// Drop a secondary index by name.
    Drop { path: PathBuf, name: String },
}

#[derive(Subcommand)]
enum UserCmd {
    /// Create a catalog user with an Argon2id-hashed password.
    Create {
        path: PathBuf,
        username: String,
        password: String,
    },
    /// Drop a user by username.
    Drop { path: PathBuf, username: String },
    /// Change a user's password.
    Passwd {
        path: PathBuf,
        username: String,
        password: String,
    },
    /// Verify credentials; prints "ok" or "invalid".
    Verify {
        path: PathBuf,
        username: String,
        password: String,
    },
    /// Grant or revoke admin privileges on a user.
    Admin {
        path: PathBuf,
        username: String,
        /// `true` to grant admin, `false` to revoke.
        granted: bool,
    },
    /// List all usernames.
    List { path: PathBuf },
}

#[derive(Subcommand)]
enum RoleCmd {
    /// Create a role.
    Create { path: PathBuf, name: String },
    /// Drop a role.
    Drop { path: PathBuf, name: String },
    /// List all role names.
    List { path: PathBuf },
    /// Grant a role to a user.
    Grant {
        path: PathBuf,
        username: String,
        role: String,
    },
    /// Revoke a role from a user.
    Revoke {
        path: PathBuf,
        username: String,
        role: String,
    },
    /// Grant a permission to a role.
    /// Permission format: `all`, `ddl`, `admin`, `select:table`,
    /// `insert:table`, `update:table`, `delete:table`.
    Allow {
        path: PathBuf,
        role: String,
        permission: String,
    },
    /// Revoke a permission from a role.
    Deny {
        path: PathBuf,
        role: String,
        permission: String,
    },
}

#[derive(Subcommand)]
enum AuthCmd {
    /// Disable require_auth on a database offline (recovery).
    DisableOffline {
        path: PathBuf,
        /// Encryption passphrase. Also via MONGREL_PASSPHRASE.
        #[arg(long, env = "MONGREL_PASSPHRASE")]
        passphrase: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    /// Enable require_auth on an existing credentialless database.
    Enable {
        path: PathBuf,
        #[arg(long)]
        admin_user: String,
        /// Admin password. Also via MONGREL_PASSWORD.
        #[arg(long, env = "MONGREL_PASSWORD")]
        admin_password: Option<String>,
    },
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

#[derive(Subcommand)]
enum ProcedureCmd {
    /// Install a procedure from a JSON file.
    Install { path: PathBuf, procedure: PathBuf },
    /// Drop a stored procedure.
    Drop { path: PathBuf, name: String },
    /// List stored procedures.
    List { path: PathBuf },
    /// Print one stored procedure as JSON.
    Describe { path: PathBuf, name: String },
    /// Call a stored procedure with optional JSON args.
    Call {
        path: PathBuf,
        name: String,
        #[arg(long)]
        args: Option<String>,
    },
}

#[derive(Clone)]
struct Credentials {
    user: String,
    password: String,
}

/// Read an environment variable, returning `None` for empty strings too.
fn resolve_credentials(cli: &Cli) -> Result<Option<Credentials>> {
    let user = match &cli.user {
        Some(u) => u.clone(),
        None => return Ok(None),
    };
    // clap's env = attribute already resolved MONGREL_PASSWORD into
    // cli.password; stdin is the fallback when neither flag nor env is set.
    let password = if let Some(pw) = &cli.password {
        pw.clone()
    } else if cli.password_stdin {
        let mut line = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut line)
            .context("failed to read password from stdin")?;
        line.trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string()
    } else {
        bail!("--user requires --password, MONGREL_PASSWORD, or --password-stdin");
    };
    Ok(Some(Credentials { user, password }))
}

fn open_db(path: &Path, creds: Option<&Credentials>) -> Result<Database> {
    match creds {
        Some(c) => Database::open_with_credentials(path, &c.user, &c.password)
            .context("failed to open database with credentials"),
        None => Database::open(path).context("failed to open database"),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let creds = resolve_credentials(&cli)?;
    match cli.command {
        Command::Init {
            path,
            require_auth,
            admin_user,
            admin_password,
        } => cmd_init(
            &path,
            require_auth,
            admin_user.as_deref(),
            admin_password.as_deref(),
        ),
        Command::Check { path } => cmd_check(&path, creds.as_ref()),
        Command::Doctor { path } => cmd_doctor(&path, creds.as_ref()),
        Command::Truncate { path, table } => cmd_truncate(&path, &table, creds.as_ref()),
        Command::Get { path, table, pk } => cmd_get(&path, &table, &pk, creds.as_ref()),
        Command::Query {
            path,
            table,
            filter,
            order,
            limit,
            offset,
            columns,
            distinct,
        } => cmd_query(
            &path,
            &table,
            filter.as_deref(),
            order.as_deref(),
            limit,
            offset,
            columns,
            distinct,
            creds.as_ref(),
        ),
        Command::Insert { path, table, row } => cmd_insert(&path, &table, &row, creds.as_ref()),
        Command::Update {
            path,
            table,
            pk,
            patch,
        } => cmd_update(&path, &table, &pk, &patch, creds.as_ref()),
        Command::Delete { path, table, pk } => cmd_delete(&path, &table, &pk, creds.as_ref()),
        Command::Upsert {
            path,
            table,
            row,
            update,
        } => cmd_upsert(&path, &table, &row, update, creds.as_ref()),
        Command::Count {
            path,
            table,
            filter,
        } => cmd_count(&path, &table, filter.as_deref(), creds.as_ref()),
        Command::Schema(cmd) => match cmd {
            SchemaCmd::Print { path } => cmd_schema_print(&path, creds.as_ref()),
            SchemaCmd::Validate { schema } => cmd_schema_validate(&schema),
        },
        Command::Migrate(cmd) => match cmd {
            MigrateCmd::Apply { path, migrations } => {
                cmd_migrate_apply(&path, &migrations, creds.as_ref())
            }
            MigrateCmd::Status { path } => cmd_migrate_status(&path, creds.as_ref()),
            MigrateCmd::Plan { path, migrations } => {
                cmd_migrate_plan(&path, &migrations, creds.as_ref())
            }
            MigrateCmd::DryRun { path, migrations } => {
                cmd_migrate_plan(&path, &migrations, creds.as_ref())
            }
        },
        Command::Diff { schema, path } => cmd_diff(&schema, &path, creds.as_ref()),
        Command::Generate(cmd) => match cmd {
            GenerateCmd::Migration { schema, from } => {
                cmd_generate_migration(&schema, &from, creds.as_ref())
            }
            GenerateCmd::Types { schema, lang } => cmd_generate_types(&schema, &lang),
        },
        Command::Fixture(cmd) => match cmd {
            FixtureCmd::Create { path, tables } => {
                cmd_fixture_create(&path, &tables, creds.as_ref())
            }
            FixtureCmd::Load { path, fixture } => cmd_fixture_load(&path, &fixture, creds.as_ref()),
        },
        Command::Procedure(cmd) => match cmd {
            ProcedureCmd::Install { path, procedure } => {
                cmd_procedure_install(&path, &procedure, creds.as_ref())
            }
            ProcedureCmd::Drop { path, name } => cmd_procedure_drop(&path, &name, creds.as_ref()),
            ProcedureCmd::List { path } => cmd_procedure_list(&path, creds.as_ref()),
            ProcedureCmd::Describe { path, name } => {
                cmd_procedure_describe(&path, &name, creds.as_ref())
            }
            ProcedureCmd::Call { path, name, args } => {
                cmd_procedure_call(&path, &name, args.as_deref(), creds.as_ref())
            }
        },
        Command::Compact { path } => cmd_compact(&path, creds.as_ref()),
        Command::Analyze { path } => cmd_analyze(&path, creds.as_ref()),
        Command::Vacuum { path } => cmd_vacuum(&path, creds.as_ref()),
        Command::RenameTable { path, from, to } => {
            cmd_rename_table(&path, &from, &to, creds.as_ref())
        }
        Command::Sql {
            path,
            statement,
            timeout_ms,
            query_id,
        } => cmd_sql(
            &path,
            &statement,
            timeout_ms,
            query_id.as_deref(),
            creds.as_ref(),
        ),
        Command::View(cmd) => match cmd {
            ViewCmd::Create { path, view } => cmd_view_create(&path, &view, creds.as_ref()),
            ViewCmd::Drop { path, name } => cmd_view_drop(&path, &name, creds.as_ref()),
        },
        Command::Index(cmd) => match cmd {
            IndexCmd::Create {
                path,
                table,
                name,
                column,
                kind,
            } => cmd_index_create(&path, &table, &name, &column, &kind, creds.as_ref()),
            IndexCmd::Drop { path, name } => cmd_index_drop(&path, &name, creds.as_ref()),
        },
        Command::User(cmd) => match cmd {
            UserCmd::Create {
                path,
                username,
                password,
            } => cmd_user_create(&path, &username, &password, creds.as_ref()),
            UserCmd::Drop { path, username } => cmd_user_drop(&path, &username, creds.as_ref()),
            UserCmd::Passwd {
                path,
                username,
                password,
            } => cmd_user_passwd(&path, &username, &password, creds.as_ref()),
            UserCmd::Verify {
                path,
                username,
                password,
            } => cmd_user_verify(&path, &username, &password, creds.as_ref()),
            UserCmd::Admin {
                path,
                username,
                granted,
            } => cmd_user_admin(&path, &username, granted, creds.as_ref()),
            UserCmd::List { path } => cmd_user_list(&path, creds.as_ref()),
        },
        Command::Role(cmd) => match cmd {
            RoleCmd::Create { path, name } => cmd_role_create(&path, &name, creds.as_ref()),
            RoleCmd::Drop { path, name } => cmd_role_drop(&path, &name, creds.as_ref()),
            RoleCmd::List { path } => cmd_role_list(&path, creds.as_ref()),
            RoleCmd::Grant {
                path,
                username,
                role,
            } => cmd_role_grant(&path, &username, &role, creds.as_ref()),
            RoleCmd::Revoke {
                path,
                username,
                role,
            } => cmd_role_revoke(&path, &username, &role, creds.as_ref()),
            RoleCmd::Allow {
                path,
                role,
                permission,
            } => cmd_role_allow(&path, &role, &permission, creds.as_ref()),
            RoleCmd::Deny {
                path,
                role,
                permission,
            } => cmd_role_deny(&path, &role, &permission, creds.as_ref()),
        },
        Command::Auth(cmd) => match cmd {
            AuthCmd::DisableOffline {
                path,
                passphrase,
                yes,
            } => cmd_auth_disable_offline(&path, passphrase.as_deref(), yes, creds.as_ref()),
            AuthCmd::Enable {
                path,
                admin_user,
                admin_password,
            } => cmd_auth_enable(&path, &admin_user, admin_password.as_deref()),
        },
    }
}

fn cmd_init(
    path: &Path,
    require_auth: bool,
    admin_user: Option<&str>,
    admin_password: Option<&str>,
) -> Result<()> {
    if require_auth {
        let admin_user = admin_user.context("--admin-user is required with --require-auth")?;
        let admin_password = admin_password
            .context("--admin-password (or MONGREL_PASSWORD) is required with --require-auth")?;
        let schema = Schema::new(Vec::new()).context("failed to build empty schema")?;
        Database::create_with_credentials(path, schema, admin_user, admin_password)
            .context("failed to create credentialed database")?;
        println!(
            "initialized {} (require_auth = true, admin user: {})",
            path.display(),
            admin_user
        );
        return Ok(());
    }

    let schema = Schema::new(Vec::new()).context("failed to build empty schema")?;
    Database::create(path, schema).context("failed to create database")?;
    println!("initialized {}", path.display());
    Ok(())
}

fn cmd_check(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    db.check_internal_tables()
        .context("internal table check failed")?;
    println!("OK: {}", path.display());
    Ok(())
}

fn cmd_compact(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let (compacted, skipped) = db.compact_all().context("compaction failed")?;
    println!("compacted {compacted} table(s), skipped {skipped}");
    Ok(())
}

fn cmd_analyze(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    db.analyze().context("analyze failed")?;
    println!("analyzed all tables");
    Ok(())
}

fn cmd_vacuum(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let reclaimed = db.vacuum().context("vacuum failed")?;
    println!("reclaimed {reclaimed} run(s)");
    Ok(())
}

fn cmd_rename_table(path: &Path, from: &str, to: &str, creds: Option<&Credentials>) -> Result<()> {
    let mut db = open_db(path, creds)?;
    db.rename_table(from, to)
        .context("failed to rename table")?;
    println!("renamed {from} -> {to}");
    Ok(())
}

fn cmd_sql(
    path: &Path,
    statement: &str,
    timeout_ms: Option<u64>,
    query_id: Option<&str>,
    creds: Option<&Credentials>,
) -> Result<()> {
    if timeout_ms == Some(0) {
        bail!("--timeout-ms must be positive");
    }
    let query_id = query_id
        .map(str::parse::<QueryId>)
        .transpose()
        .context("invalid --query-id")?;
    let db = Arc::new(open_db(path, creds)?);
    let handle = db
        .start_sql(
            statement,
            SqlOptions {
                query_id,
                timeout: timeout_ms.map(Duration::from_millis),
            },
        )
        .context("failed to start SQL")?;
    let active_id = handle.id();
    let cancel_db = Arc::clone(&db);
    ctrlc::set_handler(move || {
        cancel_db.cancel_sql(active_id);
    })
    .context("failed to install Ctrl-C SQL cancellation handler")?;
    let batches = handle.wait().context("failed to execute SQL")?;
    let rows = mongreldb_kit::arrow_util::batches_to_rows(&batches)
        .context("failed to decode SQL rows")?;
    let values: Vec<Value> = rows.into_iter().map(Value::Object).collect();
    println!("{}", serde_json::to_string_pretty(&Value::Array(values))?);
    Ok(())
}

fn cmd_view_create(path: &Path, view_path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let spec: ViewSpec = serde_json::from_reader(fs::File::open(view_path)?)
        .context("failed to read view JSON (expected {\"name\": ..., \"sql\": \"SELECT ...\"})")?;
    db.sql(&spec.create_sql())
        .context("failed to create view")?;
    println!("created view {}", spec.name);
    Ok(())
}

fn cmd_view_drop(path: &Path, name: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let sql = format!("DROP VIEW IF EXISTS {name}");
    db.sql(&sql).context("failed to drop view")?;
    println!("dropped view {name}");
    Ok(())
}

fn cmd_index_create(
    path: &Path,
    table: &str,
    name: &str,
    column: &str,
    kind: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    // Map friendly kind aliases to the engine's `CREATE INDEX ... USING <kind>`
    // vocabulary (commands.rs `index_kind_from_sql`).
    let sql_kind = match kind {
        "bitmap" => "bitmap",
        "fm" | "fm_index" => "fm",
        "ann" | "hnsw" => "ann",
        "sparse" => "sparse",
        "learned" | "brin" | "learned_range" | "range" => "brin",
        other => bail!("unknown index kind '{other}'"),
    };
    let sql = format!("CREATE INDEX {name} ON {table} ({column}) USING {sql_kind}");
    db.sql(&sql).context("failed to create index")?;
    println!("created index {name} on {table}.{column} (kind: {kind})");
    Ok(())
}

fn cmd_index_drop(path: &Path, name: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let sql = format!("DROP INDEX {name}");
    db.sql(&sql).context("failed to drop index")?;
    println!("dropped index {name}");
    Ok(())
}

// ── User / role / permission commands ──────────────────────────────────────
//
// These wrap the kit `Database` auth helpers. Permissions use the same
// `select:table` / `insert:table` / `all` / `ddl` / `admin` vocabulary as the
// NAPI and Python bindings so the CLI, Kit, and language facades agree.

fn cmd_user_create(
    path: &Path,
    username: &str,
    password: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    db.create_user(username, password)
        .context("failed to create user")?;
    println!("created user {username}");
    Ok(())
}

fn cmd_user_drop(path: &Path, username: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    db.drop_user(username).context("failed to drop user")?;
    println!("dropped user {username}");
    Ok(())
}

fn cmd_user_passwd(
    path: &Path,
    username: &str,
    password: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    db.alter_user_password(username, password)
        .context("failed to change password")?;
    println!("password changed for {username}");
    Ok(())
}

fn cmd_user_verify(
    path: &Path,
    username: &str,
    password: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let ok = db
        .verify_user(username, password)
        .context("failed to verify user")?
        .is_some();
    if ok {
        println!("ok");
    } else {
        println!("invalid");
        std::process::exit(1);
    }
    Ok(())
}

fn cmd_user_admin(
    path: &Path,
    username: &str,
    granted: bool,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    db.set_user_admin(username, granted)
        .context("failed to set admin flag")?;
    println!("admin={granted} for {username}");
    Ok(())
}

fn cmd_user_list(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let names = db.users();
    println!("{}", serde_json::to_string_pretty(&names)?);
    Ok(())
}

fn cmd_role_create(path: &Path, name: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    db.create_role(name).context("failed to create role")?;
    println!("created role {name}");
    Ok(())
}

fn cmd_role_drop(path: &Path, name: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    db.drop_role(name).context("failed to drop role")?;
    println!("dropped role {name}");
    Ok(())
}

fn cmd_role_list(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let names = db.roles();
    println!("{}", serde_json::to_string_pretty(&names)?);
    Ok(())
}

fn cmd_role_grant(
    path: &Path,
    username: &str,
    role: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    db.grant_role(username, role)
        .context("failed to grant role")?;
    println!("granted role {role} to {username}");
    Ok(())
}

fn cmd_role_revoke(
    path: &Path,
    username: &str,
    role: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    db.revoke_role(username, role)
        .context("failed to revoke role")?;
    println!("revoked role {role} from {username}");
    Ok(())
}

fn cmd_role_allow(
    path: &Path,
    role: &str,
    permission: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let perm = parse_permission(permission)?;
    db.grant_permission(role, perm)
        .context("failed to grant permission")?;
    println!("granted {permission} to role {role}");
    Ok(())
}

fn cmd_role_deny(
    path: &Path,
    role: &str,
    permission: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let perm = parse_permission(permission)?;
    db.revoke_permission(role, perm)
        .context("failed to revoke permission")?;
    println!("revoked {permission} from role {role}");
    Ok(())
}

/// Parse a permission string into a core `Permission`. Same vocabulary as the
/// NAPI and Python bindings: `all`, `ddl`, `admin`, `select:table`,
/// `insert:table`, `update:table`, `delete:table`.
fn parse_permission(s: &str) -> Result<Permission> {
    use Permission::*;
    let lower = s.to_ascii_lowercase();
    let table_of = |rest: &str| rest.trim().to_string();
    Ok(match lower.as_str() {
        "all" => All,
        "ddl" => Ddl,
        "admin" => Admin,
        _ if lower.starts_with("select:") => Select {
            table: table_of(&lower["select:".len()..]),
        },
        _ if lower.starts_with("insert:") => Insert {
            table: table_of(&lower["insert:".len()..]),
        },
        _ if lower.starts_with("update:") => Update {
            table: table_of(&lower["update:".len()..]),
        },
        _ if lower.starts_with("delete:") => Delete {
            table: table_of(&lower["delete:".len()..]),
        },
        other => bail!(
            "unknown permission '{other}' (expected all, ddl, admin, select:table, insert:table, update:table, delete:table)"
        ),
    })
}

fn cmd_procedure_install(
    path: &Path,
    procedure_path: &Path,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let value: Value = serde_json::from_reader(fs::File::open(procedure_path)?)
        .context("failed to read procedure JSON")?;
    let spec = ProcedureSpec::new(value);
    let proc = db
        .replace_procedure(&spec)
        .context("failed to install procedure")?;
    println!("{}", serde_json::to_string_pretty(&proc)?);
    Ok(())
}

fn cmd_procedure_drop(path: &Path, name: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    db.drop_procedure(name)
        .context("failed to drop procedure")?;
    println!("dropped {name}");
    Ok(())
}

fn cmd_procedure_list(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let names: Vec<String> = db.raw().procedures().into_iter().map(|p| p.name).collect();
    println!("{}", serde_json::to_string_pretty(&names)?);
    Ok(())
}

fn cmd_procedure_describe(path: &Path, name: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let proc = db
        .raw()
        .procedure(name)
        .with_context(|| format!("procedure {name:?} not found"))?;
    println!("{}", serde_json::to_string_pretty(&proc)?);
    Ok(())
}

fn cmd_procedure_call(
    path: &Path,
    name: &str,
    args: Option<&str>,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let args: Map<String, Value> = match args {
        Some(args) => serde_json::from_str(args).context("failed to parse args JSON")?,
        None => Map::new(),
    };
    let result = db
        .call_procedure(name, args)
        .context("failed to call procedure")?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn cmd_doctor(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
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

fn cmd_truncate(path: &Path, table: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let mut txn = db.begin().context("failed to begin transaction")?;
    txn.truncate(table)
        .context(format!("failed to truncate {table}"))?;
    txn.commit().context("failed to commit transaction")?;
    let _ = db.close(); // §4.4: flush-on-close
    println!("table {table} truncated");
    Ok(())
}

// ── Data commands ──────────────────────────────────────────────────────────
//
// Scalars (primary keys) are parsed as JSON with a string fallback, so `5` is an
// integer and `alice` is the text "alice" without shell quoting. Rows/patches
// are JSON objects. Filters use the same friendly `{"col": {"op": value}}` shape
// the language facades accept, so the CLI, Kit, and conformance runners agree.

fn parse_scalar(arg: &str) -> Value {
    serde_json::from_str(arg).unwrap_or_else(|_| Value::String(arg.to_string()))
}

fn parse_object(arg: &str, what: &str) -> Result<Map<String, Value>> {
    match serde_json::from_str(arg).context(format!("failed to parse {what} JSON"))? {
        Value::Object(m) => Ok(m),
        _ => bail!("{what} must be a JSON object"),
    }
}

fn value_to_literal(v: &Value) -> Literal {
    match v {
        Value::Null => Literal::Null,
        Value::Bool(b) => Literal::Bool(*b),
        Value::Number(n) => n
            .as_i64()
            .map(Literal::Int)
            .unwrap_or_else(|| Literal::Float(n.as_f64().unwrap_or(f64::NAN))),
        Value::String(s) => Literal::Text(s.clone()),
        other => Literal::Json(other.clone()),
    }
}

fn literal_list(v: &Value) -> Result<Vec<Literal>> {
    Ok(v.as_array()
        .context("in/not_in expects an array")?
        .iter()
        .map(value_to_literal)
        .collect())
}

/// Translate a friendly per-column filter object into an engine `Expr`. Ops:
/// eq/ne/gt/gte/lt/lte/like/contains/bytes_prefix/in/not_in/is_null/is_not_null;
/// `{"col": value}` is shorthand for eq; multiple keys are AND-ed.
fn parse_filter(map: &Map<String, Value>) -> Result<Expr> {
    let mut parts = Vec::new();
    for (col, val) in map {
        let col_expr = || Box::new(Expr::Column(col.clone()));
        let lit = |v: &Value| Box::new(Expr::Literal(value_to_literal(v)));
        let expr = match val {
            Value::Object(op) if op.len() == 1 => {
                let (name, operand) = op.iter().next().unwrap();
                match name.as_str() {
                    "eq" => Expr::Eq(col_expr(), lit(operand)),
                    "ne" => Expr::Ne(col_expr(), lit(operand)),
                    "gt" => Expr::Gt(col_expr(), lit(operand)),
                    "gte" => Expr::Gte(col_expr(), lit(operand)),
                    "lt" => Expr::Lt(col_expr(), lit(operand)),
                    "lte" => Expr::Lte(col_expr(), lit(operand)),
                    "like" => Expr::Like(col_expr(), filter_str(operand, "like")?),
                    "contains" => Expr::Contains(col_expr(), filter_str(operand, "contains")?),
                    "bytes_prefix" => {
                        Expr::BytesPrefix(col_expr(), filter_str(operand, "bytes_prefix")?)
                    }
                    "in" => Expr::In(col_expr(), literal_list(operand)?),
                    "not_in" => Expr::NotIn(col_expr(), literal_list(operand)?),
                    "is_null" => Expr::IsNull(col_expr()),
                    "is_not_null" => Expr::IsNotNull(col_expr()),
                    other => bail!("unknown filter operator {other}"),
                }
            }
            Value::Null => Expr::IsNull(col_expr()),
            other => Expr::Eq(col_expr(), lit(other)),
        };
        parts.push(expr);
    }
    Ok(match parts.len() {
        0 => Expr::Literal(Literal::Bool(true)),
        1 => parts.into_iter().next().unwrap(),
        _ => Expr::And(parts),
    })
}

/// Extract a string operand for the `like`/`contains`/`bytes_prefix` filter ops.
fn filter_str(v: &Value, op: &str) -> Result<String> {
    match v.as_str() {
        Some(s) => Ok(s.to_string()),
        None => bail!("operator {op} expects a string operand"),
    }
}

fn parse_order(order: &str) -> Vec<OrderBy> {
    order
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }
            let (direction, col) = match part.strip_prefix('-') {
                Some(rest) => (Direction::Desc, rest),
                None => (Direction::Asc, part.strip_prefix('+').unwrap_or(part)),
            };
            Some(OrderBy {
                expr: Expr::Column(col.to_string()),
                direction,
            })
        })
        .collect()
}

fn optional_filter(filter: Option<&str>) -> Result<Option<Expr>> {
    match filter {
        Some(s) => Ok(Some(parse_filter(&parse_object(s, "filter")?)?)),
        None => Ok(None),
    }
}

fn cmd_get(path: &Path, table: &str, pk: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let txn = db.begin().context("failed to begin transaction")?;
    let row = txn
        .get_by_pk(table, &parse_scalar(pk))
        .context(format!("failed to read from {table}"))?;
    match row {
        Some(r) => println!(
            "{}",
            serde_json::to_string_pretty(&Value::Object(r.values))?
        ),
        None => println!("null"),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cmd_query(
    path: &Path,
    table: &str,
    filter: Option<&str>,
    order: Option<&str>,
    limit: Option<usize>,
    offset: Option<usize>,
    columns: Option<Vec<String>>,
    distinct: bool,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let txn = db.begin().context("failed to begin transaction")?;
    let select = Select {
        table: table.to_string(),
        columns: columns
            .unwrap_or_default()
            .into_iter()
            .map(Expr::Column)
            .collect(),
        filter: optional_filter(filter)?,
        order_by: order.map(parse_order).unwrap_or_default(),
        limit,
        offset,
    };
    let query = Query::Select(select);
    let rows = if distinct {
        txn.select_distinct(&query)
    } else {
        txn.select(&query)
    }
    .context(format!("failed to query {table}"))?;
    let values: Vec<Value> = rows.into_iter().map(|r| Value::Object(r.values)).collect();
    println!("{}", serde_json::to_string_pretty(&Value::Array(values))?);
    Ok(())
}

fn cmd_insert(path: &Path, table: &str, row: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let mut txn = db.begin().context("failed to begin transaction")?;
    let inserted = txn
        .insert(table, parse_object(row, "row")?)
        .context(format!("failed to insert into {table}"))?;
    txn.commit().context("failed to commit transaction")?;
    let _ = db.close(); // §4.4: flush-on-close
    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Object(inserted.values))?
    );
    Ok(())
}

fn cmd_update(
    path: &Path,
    table: &str,
    pk: &str,
    patch: &str,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let mut txn = db.begin().context("failed to begin transaction")?;
    let updated = txn
        .update(table, &parse_scalar(pk), parse_object(patch, "patch")?)
        .context(format!("failed to update {table}"))?;
    txn.commit().context("failed to commit transaction")?;
    let _ = db.close(); // §4.4: flush-on-close
    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Object(updated.values))?
    );
    Ok(())
}

fn cmd_delete(path: &Path, table: &str, pk: &str, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
    let mut txn = db.begin().context("failed to begin transaction")?;
    txn.delete(table, &parse_scalar(pk))
        .context(format!("failed to delete from {table}"))?;
    txn.commit().context("failed to commit transaction")?;
    let _ = db.close(); // §4.4: flush-on-close
    println!("deleted");
    Ok(())
}

fn cmd_upsert(
    path: &Path,
    table: &str,
    row: &str,
    update: bool,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let mut txn = db.begin().context("failed to begin transaction")?;
    let row_map = parse_object(row, "row")?;
    let returning: Vec<String> = row_map.keys().cloned().collect();
    let on_conflict = if update {
        OnConflict::DoUpdate(row_map.clone())
    } else {
        OnConflict::DoNothing
    };
    let result = txn
        .upsert(table, row_map, on_conflict, returning)
        .context(format!("failed to upsert into {table}"))?;
    txn.commit().context("failed to commit transaction")?;
    let _ = db.close(); // §4.4: flush-on-close
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn cmd_count(
    path: &Path,
    table: &str,
    filter: Option<&str>,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
    let txn = db.begin().context("failed to begin transaction")?;
    let query = AggregateQuery {
        table: table.to_string(),
        filter: optional_filter(filter)?,
        group_by: Vec::new(),
        aggregates: vec![Aggregate {
            func: AggFunc::Count,
            column: None,
            alias: "count".to_string(),
            distinct: false,
        }],
        having: None,
    };
    let rows = txn
        .aggregate(&query)
        .context(format!("failed to count {table}"))?;
    let count = rows
        .first()
        .and_then(|r| r.values.get("count"))
        .cloned()
        .unwrap_or_else(|| Value::from(0));
    println!("{count}");
    Ok(())
}

fn cmd_schema_print(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
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

fn cmd_migrate_apply(
    path: &Path,
    migrations_path: &Path,
    creds: Option<&Credentials>,
) -> Result<()> {
    let mut db = open_db(path, creds)?;
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

fn cmd_migrate_status(path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
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

fn cmd_migrate_plan(
    path: &Path,
    migrations_path: &Path,
    creds: Option<&Credentials>,
) -> Result<()> {
    let db = open_db(path, creds)?;
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

fn cmd_diff(schema_path: &Path, path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let code = read_schema(schema_path)?;
    let db = open_db(path, creds)?;
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

fn cmd_generate_migration(
    schema_path: &Path,
    from: &Path,
    creds: Option<&Credentials>,
) -> Result<()> {
    let code = read_schema(schema_path)?;
    let db = open_db(from, creds)?;
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
        ColumnType::Text
        | ColumnType::Date
        | ColumnType::DateTime
        | ColumnType::Date64
        | ColumnType::Time64
        | ColumnType::Interval
        | ColumnType::Decimal128
        | ColumnType::Uuid
        | ColumnType::JsonNative
        | ColumnType::Array => "string",
        ColumnType::Bytes => "Uint8Array",
        ColumnType::Json => "unknown",
        ColumnType::Embedding => "number[]",
        ColumnType::Sparse => "[number, number][]",
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
        ColumnType::Text
        | ColumnType::Date
        | ColumnType::DateTime
        | ColumnType::Date64
        | ColumnType::Time64
        | ColumnType::Interval
        | ColumnType::Decimal128
        | ColumnType::Uuid
        | ColumnType::JsonNative
        | ColumnType::Array => "String",
        ColumnType::Bytes => "Vec<u8>",
        ColumnType::Json => "serde_json::Value",
        ColumnType::Embedding => "Vec<f32>",
        ColumnType::Sparse => "Vec<(u32, f32)>",
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
        ColumnType::Text
        | ColumnType::Date
        | ColumnType::DateTime
        | ColumnType::Date64
        | ColumnType::Time64
        | ColumnType::Interval
        | ColumnType::Decimal128
        | ColumnType::Uuid
        | ColumnType::JsonNative
        | ColumnType::Array => "str",
        ColumnType::Bytes => "bytes",
        ColumnType::Json => "Any",
        ColumnType::Embedding => "list[float]",
        ColumnType::Sparse => "list[tuple[int, float]]",
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

fn cmd_fixture_create(path: &Path, tables: &[String], creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
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
    let _ = db.close(); // §4.4: flush-on-close

    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn cmd_fixture_load(path: &Path, fixture_path: &Path, creds: Option<&Credentials>) -> Result<()> {
    let db = open_db(path, creds)?;
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
    let _ = db.close(); // §4.4: flush-on-close
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

// ── Auth enforcement commands ──────────────────────────────────────────────
//
// `enable` flips require_auth on an existing credentialless database via the
// kit `Database::enable_auth` helper. `disable-offline` is a recovery path
// that calls the engine's in-process `disable_auth` on an already-opened
// database; the operator must have filesystem access (and, for encrypted or
// `require_auth` databases, the passphrase or admin credentials). For the
// lost-credentials case, see docs/15-credential-enforcement.md §4.7.

fn cmd_auth_enable(path: &Path, admin_user: &str, admin_password: Option<&str>) -> Result<()> {
    let admin_password =
        admin_password.context("--admin-password (or MONGREL_PASSWORD) is required")?;
    let db = Database::open(path).context("failed to open database")?;
    db.enable_auth(admin_user, admin_password)
        .context("failed to enable require_auth")?;
    println!("require_auth enabled, admin user: {admin_user}");
    Ok(())
}

fn cmd_auth_disable_offline(
    path: &Path,
    passphrase: Option<&str>,
    yes: bool,
    creds: Option<&Credentials>,
) -> Result<()> {
    eprintln!("WARNING: disabling require_auth on {}", path.display());
    eprintln!("This reverts the database to credentialless mode. Users and roles are preserved");
    eprintln!("but no longer enforced. Anyone with filesystem access can read the data.");
    if !yes {
        eprintln!();
        eprint!("proceed? [y/N] ");
        let mut line = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut line)
            .context("failed to read confirmation from stdin")?;
        if !line
            .trim_start()
            .chars()
            .next()
            .map(|c| c.eq_ignore_ascii_case(&'y'))
            .unwrap_or(false)
        {
            println!("aborted");
            return Ok(());
        }
    }
    // Open the database (plain or encrypted) and call disable_auth. This is the
    // recovery path: the caller must have filesystem access to the database
    // directory. For a `require_auth` database, a credentialed open is needed
    // — but the whole point of recovery is that credentials may be lost. In
    // that case, the operator opens the catalog file directly and flips the
    // flag (see docs/15-credential-enforcement.md §4.7). The CLI provides this
    // command for the common case: the admin password is known but the operator
    // wants to revert to credentialless mode without a separate credentialed
    // session.
    let db = match passphrase {
        Some(pw) => {
            // Encrypted database — try with passphrase (may also need credentials).
            if let Some(c) = creds {
                Database::open_encrypted_with_credentials(path, pw, &c.user, &c.password)
                    .context("failed to open encrypted database with credentials")
            } else {
                Database::open_encrypted(path, pw).context("failed to open encrypted database")
            }
        }
        None => {
            // Plain database — try open, fall back to credentialed if global
            // --user/--password were supplied.
            match Database::open(path) {
                Ok(db) => Ok(db),
                Err(_) if creds.is_some() => {
                    let c = creds.unwrap();
                    Database::open_with_credentials(path, &c.user, &c.password)
                        .context("failed to open database with credentials")
                }
                Err(_) => {
                    bail!(
                        "cannot open database (it may require_auth or be encrypted). \
                         Pass --user/--password for credentialed databases, or --passphrase \
                         for encrypted databases. For require_auth databases where credentials \
                         are lost, see docs/15-credential-enforcement.md §4.7 for offline recovery."
                    );
                }
            }
        }
    }?;
    db.disable_auth()
        .context("failed to disable require_auth")?;
    println!("require_auth disabled — database is now credentialless");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn scalar_arg_parses_as_json_with_string_fallback() {
        assert_eq!(parse_scalar("5"), Value::from(5));
        assert_eq!(parse_scalar("true"), Value::from(true));
        // Bare, unquoted text is treated as a string, not a JSON parse error.
        assert_eq!(parse_scalar("alice"), Value::from("alice"));
    }

    #[test]
    fn friendly_filter_translates_to_expr() {
        let col = |c: &str| Box::new(Expr::Column(c.to_string()));
        let int = |n: i64| Box::new(Expr::Literal(Literal::Int(n)));

        assert_eq!(
            parse_filter(&obj(json!({"amount": {"gte": 100}}))).unwrap(),
            Expr::Gte(col("amount"), int(100))
        );
        // Bare value is shorthand for eq.
        assert_eq!(
            parse_filter(&obj(json!({"region": "east"}))).unwrap(),
            Expr::Eq(
                col("region"),
                Box::new(Expr::Literal(Literal::Text("east".into())))
            )
        );
        // Multiple keys AND together.
        assert!(matches!(
            parse_filter(&obj(json!({"a": 1, "b": 2}))).unwrap(),
            Expr::And(parts) if parts.len() == 2
        ));
        assert_eq!(
            parse_filter(&obj(json!({"name": {"is_null": true}}))).unwrap(),
            Expr::IsNull(col("name"))
        );
    }

    #[test]
    fn order_spec_parses_direction_prefixes() {
        let ords = parse_order("+id,-created");
        assert_eq!(ords.len(), 2);
        assert!(matches!(ords[0].direction, Direction::Asc));
        assert!(matches!(ords[1].direction, Direction::Desc));
    }
}
