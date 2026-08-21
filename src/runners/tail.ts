import { open, type FileHandle } from 'node:fs/promises';

/**
 * Follow a file that another process is appending to, emitting complete lines.
 *
 * Used instead of piping k6's `--out json=-` through stdout: k6 interleaves its
 * own console output on stdout, which corrupts NDJSON parsing.
 */
export async function tailLines(
  path: string,
  onLine: (line: string) => void,
  opts: { pollMs?: number; done: () => boolean },
): Promise<void> {
  const pollMs = opts.pollMs ?? 200;
  let fh: FileHandle | null = null;
  let pos = 0;
  let carry = '';
  const buf = Buffer.alloc(1 << 20);

  const drain = async (): Promise<void> => {
    if (!fh) return;
    for (;;) {
      const { bytesRead } = await fh.read(buf, 0, buf.length, pos);
      if (bytesRead === 0) return;
      pos += bytesRead;
      carry += buf.subarray(0, bytesRead).toString('utf8');
      const lines = carry.split('\n');
      carry = lines.pop() ?? '';
      for (const l of lines) if (l) onLine(l);
    }
  };

  try {
    for (;;) {
      if (!fh) {
        try { fh = await open(path, 'r'); } catch { /* not created yet */ }
      }
      await drain();
      if (opts.done()) { await drain(); break; }
      await new Promise((r) => setTimeout(r, pollMs));
    }
    if (carry) onLine(carry);
  } finally {
    await fh?.close();
  }
}
