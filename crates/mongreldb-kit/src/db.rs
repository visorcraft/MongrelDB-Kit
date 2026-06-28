//! Database handle for `mongreldb-kit`.

use crate::error::{KitError, Result};
use crate::migrate::MIGRATIONS_TABLE;
use crate::schema::to_core_schema;
use mongreldb_core::Database as CoreDatabase;
use mongreldb_core::memtable::Row as CoreRow;
use mongreldb_core::schema::{Schema as CoreSchema, TypeId};
use mongreldb_kit_core::schema::Schema as KitSchema;
use mongreldb_kit_core::schema::Table as KitTable;

use std::path::{Path, PathBuf};

const SCHEMA_FILE: &str = "kit_schema.json";

/// A kit database handle.
///
/// Wraps a MongrelDB core database and a kit schema, providing table metadata
/// and transaction creation.
pub struct Database {
    pub(crate) inner: CoreDatabase,
    pub(crate) schema: KitSchema,
    pub(crate) root: PathBuf,
}

impl Database {
    /// Open an existing kit database.
    pub fn open(path: &Path) -> Result<Self> {
        let inner = CoreDatabase::open(path)?;
        let schema = load_schema(path)?;
        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
        })
    }

    /// Create a fresh kit database with the given schema.
    pub fn create(path: &Path, schema: KitSchema) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let inner = CoreDatabase::create(path)?;

        // Create internal tables first so we can record migrations.
        create_core_table(&inner, MIGRATIONS_TABLE, migrations_table_core())?;

        // Persist the kit schema to a sidecar file (core tables cannot update
        // a specific row by id, so a file is the pragmatic stable store).
        store_schema(path, &schema)?;

        // Create core tables for every user table.
        for table in &schema.tables {
            create_core_table(&inner, &table.name, to_core_schema(table))?;
        }

        Ok(Self {
            inner,
            schema,
            root: path.to_path_buf(),
        })
    }

    /// Look up a table definition by name.
    pub fn table(&self, name: &str) -> Option<&KitTable> {
        self.schema.table(name)
    }

    /// Begin a new kit transaction.
    pub fn begin(&self) -> Result<crate::txn::Transaction<'_>> {
        let core_txn = self.inner.begin();
        Ok(crate::txn::Transaction::new(self, core_txn))
    }

    /// Replace the in-memory schema, usually after a migration.
    pub fn set_schema(&mut self, schema: KitSchema) {
        self.schema = schema;
    }

    pub(crate) fn core_db(&self) -> &CoreDatabase {
        &self.inner
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// All visible core rows for a table at the current snapshot.
    pub(crate) fn visible_core_rows(&self, table_name: &str) -> Result<Vec<CoreRow>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let guard = handle.lock();
        let snapshot = guard.snapshot();
        guard.visible_rows(snapshot).map_err(KitError::from)
    }

    /// Materialize a single row by storage row id.
    #[allow(dead_code)]
    pub(crate) fn get_core_row(&self, table_name: &str, row_id: u64) -> Result<Option<CoreRow>> {
        let handle = self.inner.table(table_name).map_err(KitError::from)?;
        let guard = handle.lock();
        let snapshot = guard.snapshot();
        Ok(guard.get(mongreldb_core::RowId(row_id), snapshot))
    }
}

pub(crate) fn migrations_table_core() -> CoreSchema {
    CoreSchema {
        schema_id: u64::MAX - 1,
        columns: vec![
            mongreldb_core::schema::ColumnDef {
                id: 1,
                name: "version".into(),
                ty: TypeId::Int64,
                flags: mongreldb_core::schema::ColumnFlags::empty()
                    .with(mongreldb_core::schema::ColumnFlags::PRIMARY_KEY),
            },
            mongreldb_core::schema::ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: mongreldb_core::schema::ColumnFlags::empty(),
            },
            mongreldb_core::schema::ColumnDef {
                id: 3,
                name: "checksum".into(),
                ty: TypeId::Bytes,
                flags: mongreldb_core::schema::ColumnFlags::empty(),
            },
            mongreldb_core::schema::ColumnDef {
                id: 4,
                name: "applied_at".into(),
                ty: TypeId::Bytes,
                flags: mongreldb_core::schema::ColumnFlags::empty(),
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
    }
}

fn create_core_table(db: &CoreDatabase, name: &str, schema: CoreSchema) -> Result<()> {
    if db.table_id(name).is_ok() {
        return Ok(());
    }
    db.create_table(name, schema).map_err(KitError::from)?;
    Ok(())
}

pub(crate) fn load_schema(path: &Path) -> Result<KitSchema> {
    let file = path.join(SCHEMA_FILE);
    let json = std::fs::read_to_string(&file)
        .map_err(|e| KitError::Migration(format!("cannot read schema file: {e}")))?;
    let schema: KitSchema = serde_json::from_str(&json)?;
    Ok(schema)
}

pub(crate) fn store_schema(path: &Path, schema: &KitSchema) -> Result<()> {
    let file = path.join(SCHEMA_FILE);
    let json = serde_json::to_string_pretty(schema)?;
    std::fs::write(&file, json)?;
    Ok(())
}

/// Persist a kit schema into the database. Used after migrations.
pub(crate) fn persist_schema(db: &Database, schema: &KitSchema) -> Result<()> {
    store_schema(&db.root, schema)
}
