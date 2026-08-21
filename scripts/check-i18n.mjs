/**
 * Fails when the locale files drift apart. A missing Thai key silently falls
 * back to English in the UI, which is easy to ship without noticing.
 */
import { readFileSync } from 'node:fs';

const load = (l) => JSON.parse(readFileSync(new URL(`../web/src/i18n/locales/${l}.json`, import.meta.url), 'utf8'));

function keys(obj, prefix = '') {
  return Object.entries(obj).flatMap(([k, v]) => {
    const path = prefix ? `${prefix}.${k}` : k;
    return v && typeof v === 'object' ? keys(v, path) : [path];
  });
}

const en = new Set(keys(load('en')));
const th = new Set(keys(load('th')));
const missing = [...en].filter((k) => !th.has(k));
const extra = [...th].filter((k) => !en.has(k));

if (missing.length || extra.length) {
  if (missing.length) console.error(`Missing in th.json:\n  ${missing.join('\n  ')}`);
  if (extra.length) console.error(`Not in en.json:\n  ${extra.join('\n  ')}`);
  process.exit(1);
}
console.log(`i18n OK — ${en.size} keys in both locales`);
