import type { Cell, RowJs } from 'mongreldb/native.js';
import type { TableSpec } from './types.js';

export function cellValue(cell: Cell | undefined): unknown {
	if (!cell) return null;
	if (cell.text !== undefined) return cell.text;
	if (cell.int64 !== undefined) return cell.int64;
	if (cell.boolean !== undefined) return cell.boolean;
	if (cell.float64 !== undefined) return cell.float64;
	if (cell.bytes !== undefined) return cell.bytes;
	return null;
}

export function rowFromRowJs(table: TableSpec, rowJs: RowJs): Record<string, unknown> {
	const row: Record<string, unknown> = {};
	const cells = rowJs.cells;
	let aligned = cells.length >= table.columns.length;
	for (let i = 0; aligned && i < table.columns.length; i++) {
		aligned = cells[i]?.columnId === table.columns[i]!.id;
	}
	if (aligned) {
		for (let i = 0; i < table.columns.length; i++) {
			const col = table.columns[i]!;
			row[col.name] = cellValue(cells[i]);
		}
		return row;
	}

	const byColumn = new Map<number, Cell>();
	for (const cell of cells) byColumn.set(cell.columnId, cell);
	for (const col of table.columns) {
		row[col.name] = cellValue(byColumn.get(col.id));
	}
	return row;
}
