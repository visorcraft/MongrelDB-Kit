# Production Checklist

This checklist covers the basics of running MongrelDB Kit in production.

## Environment variables

| Variable | Used by | Recommendation |
|---|---|---|
| `MONGREL_DATABASE_PATH` / `DATABASE_PATH` | TypeScript kit | Single persistent path; mount as a volume in containers |
| `ROAMARR_SECRET` | Encrypted-at-rest fields in applications | Generate once, back up offline, reuse across container recreations |
| `PORT` | adapter-node / servers | Set explicitly; default is `3000` |
| `ORIGIN` | Servers behind proxies | Set to the public origin for cookies and redirects |

## Backup

MongrelDB stores data in a directory or file. Back up the resolved database path and the `attachments/` directory beside it.

- Stop the application or use a filesystem snapshot for a consistent backup.
- Copy the data directory with `cp -a`, `rsync`, or a volume snapshot.
- Test restores on a non-production instance.
- Never reuse a backup with a different `ROAMARR_SECRET`; encrypted fields will be unrecoverable.

## Monitoring

Monitor these signals:

- Disk space on the database volume
- Migration lock age in `__kit_migration_locks`
- Query latency for full-table scans (the kit currently materializes all visible rows for unindexed filters)
- Error rates by category: `DUPLICATE`, `FOREIGN_KEY`, `RESTRICT`, `VALIDATION`, `MIGRATION`
- Health endpoint: `GET /health` for adapter-node deployments

## Performance

- Index columns used in equality filters and joins.
- Avoid large unfiltered full-table scans in hot paths.
- Keep transactions short to reduce conflict retries.
- Use batch inserts (`valuesMany` / `insert_many`) for bulk loads — one transaction is far
  cheaper than a row-at-a-time loop.

## Migrations

- Always run migrations before starting application servers.
- Run migrations from one process at a time; the advisory lock prevents collisions.
- Test migrations against a copy of production data in staging.
- Keep migration names stable; the checksum is derived from `version:name`.

## Security

- Do not log full rows or encoded guard keys.
- Do not expose `__kit_` tables through application APIs.
- Validate any use of raw escape hatches (`nativeDb`, `db.inner`, `db._handle`).
- Rotate secrets only when the kit explicitly supports re-encryption.

## Upgrades

1. Read the changelog and migration compatibility notes.
2. Back up the database.
3. Deploy the new kit version.
4. Run migrations.
5. Verify `/health` and smoke tests.

## Disaster recovery

- Store `ROAMARR_SECRET` separately from the database backup.
- Document the exact kit version used to write the database.
- Practice a restore at least once per release cycle.
