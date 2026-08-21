import { access, constants, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Protocol, ScriptConfig } from '../shared/types.ts';

const HERE = dirname(fileURLToPath(import.meta.url));

const EXAMPLES: Record<Protocol, string> = {
  rest: join(HERE, 'examples', 'k6.example.js'),
  socket: join(HERE, 'examples', 'artillery.example.yml'),
  kafka: join(HERE, 'examples', 'kafka.example.mjs'),
};

export async function exampleScript(protocol: Protocol): Promise<string> {
  return readFile(EXAMPLES[protocol], 'utf8');
}

export function usesCustomScript(script: ScriptConfig | undefined): script is ScriptConfig {
  return !!script && script.mode !== 'builtin';
}

/**
 * Resolve a custom script to a concrete file to execute.
 *
 * `path` scripts run where they live so relative imports, `open()` and CSV data
 * feeds keep resolving. `inline` scripts are written into the run's temp dir,
 * which is why they have to be self-contained.
 */
export async function materializeScript(
  script: ScriptConfig,
  tmpDir: string,
  fallbackName: string,
): Promise<string> {
  if (script.mode === 'path') {
    try {
      await access(script.path, constants.R_OK);
    } catch {
      throw new Error(`script not readable: ${script.path}`);
    }
    return script.path;
  }
  const name = safeName(script.filename) || fallbackName;
  const target = join(tmpDir, name);
  await writeFile(target, script.content, 'utf8');
  return target;
}

/** Filename is display metadata from the browser — never let it escape tmpDir. */
function safeName(filename: string): string {
  const base = filename.replace(/^.*[\\/]/, '').trim();
  return /^[\w.-]{1,120}$/.test(base) && base !== '.' && base !== '..' ? base : '';
}
