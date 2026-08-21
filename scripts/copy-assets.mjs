/**
 * tsc only emits TypeScript. The k6 driver script and the Artillery plugin are
 * plain JS assets loaded from disk at runtime, so a built server without them
 * fails the moment someone presses Run.
 */
import { cp, access } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const DIRS = ['assets', 'examples'];

for (const dir of DIRS) {
  const src = fileURLToPath(new URL(`../src/runners/${dir}`, import.meta.url));
  const dest = fileURLToPath(new URL(`../dist/runners/${dir}`, import.meta.url));
  await access(src);
  await cp(src, dest, { recursive: true });
  console.log(`copied runner ${dir} → ${dest}`);
}
