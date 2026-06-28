import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		include: ['src/**/*.test.ts', '../../tests/conformance/typescript/**/*.test.ts'],
		passWithNoTests: true
	}
});
