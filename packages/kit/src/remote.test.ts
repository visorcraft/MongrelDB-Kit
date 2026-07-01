import { describe, it, expect } from 'vitest';
import { RemoteDatabase } from './remote.js';

describe('RemoteDatabase', () => {
	it('constructs lazily and surfaces connection errors on use', () => {
		// Construction does not connect (lazy agent), so this never throws.
		const remote = new RemoteDatabase('http://127.0.0.1:9');
		// Port 9 (discard) has no daemon, so an actual request throws.
		expect(() => remote.health()).toThrow();
	});
});
