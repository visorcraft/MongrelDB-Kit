import { Buffer } from 'node:buffer';
import type { TableSpec } from './types.js';

// Wire format for Transaction.putPacked / deletePacked (little-endian). Mirrors
// the decoder in mongreldb-node/src/lib.rs. Packing rows into one buffer avoids
// the per-cell NAPI `Cell` object marshalling that dominates a bulk load.
//
//   put:    u32 rowCount, per row u16 cellCount, per cell u16 columnId + u8 tag
//           + payload. tag 0=null, 1=int64 (i64 LE), 2=float64 (f64 LE),
//           3=bool (u8), 4=bytes (u32 len + bytes; text is UTF-8).
//   delete: u32 count, then count × u64 LE row id.

class ByteWriter {
	private buf: Buffer;
	private pos = 0;
	constructor(initial: number) {
		this.buf = Buffer.allocUnsafe(Math.max(initial, 16));
	}
	private ensure(n: number): void {
		if (this.pos + n <= this.buf.length) return;
		let next = this.buf.length * 2;
		while (next < this.pos + n) next *= 2;
		const grown = Buffer.allocUnsafe(next);
		this.buf.copy(grown, 0, 0, this.pos);
		this.buf = grown;
	}
	u8(v: number): void {
		this.ensure(1);
		this.buf.writeUInt8(v, this.pos);
		this.pos += 1;
	}
	u16(v: number): void {
		this.ensure(2);
		this.buf.writeUInt16LE(v, this.pos);
		this.pos += 2;
	}
	u32(v: number): void {
		this.ensure(4);
		this.buf.writeUInt32LE(v, this.pos);
		this.pos += 4;
	}
	i64(v: bigint): void {
		this.ensure(8);
		this.buf.writeBigInt64LE(v, this.pos);
		this.pos += 8;
	}
	u64(v: bigint): void {
		this.ensure(8);
		this.buf.writeBigUInt64LE(v, this.pos);
		this.pos += 8;
	}
	f64(v: number): void {
		this.ensure(8);
		this.buf.writeDoubleLE(v, this.pos);
		this.pos += 8;
	}
	bytes(b: Uint8Array): void {
		this.u32(b.length);
		this.ensure(b.length);
		Buffer.from(b.buffer, b.byteOffset, b.byteLength).copy(this.buf, this.pos);
		this.pos += b.length;
	}
	text(s: string): void {
		const len = Buffer.byteLength(s, 'utf8');
		this.u32(len);
		this.ensure(len);
		this.pos += this.buf.write(s, this.pos, 'utf8');
	}
	result(): Buffer {
		return this.buf.subarray(0, this.pos);
	}
}

/**
 * Pack a batch of already-defaulted rows for one table into a putPacked buffer.
 * Emits every column in `table.columns` order, mirroring `toCells` (text/json/
 * date/timestamp -> UTF-8 bytes; null/undefined -> SQL NULL), so the decoded
 * `(columnId, Value)` pairs are identical to the per-row `put` path.
 */
export function packRows(table: TableSpec, rows: Record<string, unknown>[]): Buffer {
	const w = new ByteWriter(rows.length * 64 + 4);
	w.u32(rows.length);
	for (const row of rows) {
		w.u16(table.columns.length);
		for (const col of table.columns) {
			w.u16(col.id);
			const value = row[col.name];
			if (value === null || value === undefined) {
				w.u8(0); // null
				continue;
			}
			switch (col.storageType) {
				case 'bool':
					w.u8(3);
					w.u8(value ? 1 : 0);
					break;
				case 'int64':
					w.u8(1);
					w.i64(value as bigint);
					break;
				case 'float64':
					w.u8(2);
					w.f64(value as number);
					break;
				case 'bytes':
					w.u8(4);
					w.bytes(value as Uint8Array);
					break;
				case 'text':
				case 'timestamp':
				case 'date':
				case 'json':
					w.u8(4);
					w.text(value as string);
					break;
				case 'embedding':
					// The compact bulk format has no vector tag; embedding columns
					// use the per-row insert path (toCells) instead.
					throw new Error('embedding columns are not supported in bulk insert');
				default: {
					const _exhaustive: never = col.storageType;
					throw new Error(`Unsupported storage type for packing: ${_exhaustive}`);
				}
			}
		}
	}
	return w.result();
}

/** Pack a list of row ids into a deletePacked buffer. */
export function packRowIds(ids: bigint[]): Buffer {
	const w = new ByteWriter(ids.length * 8 + 4);
	w.u32(ids.length);
	for (const id of ids) w.u64(id);
	return w.result();
}
