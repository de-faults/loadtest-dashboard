import { parse as yamlParse } from 'yaml';
import type { KafkaAuth } from '../shared/types.ts';

/**
 * Broker connection settings arrive in whatever shape the team already keeps
 * them in: a librdkafka `client.properties`, a Java properties file with a JAAS
 * line, a KafkaJS options object, a Helm values fragment, or a `.env` with
 * KAFKA_* variables. All of them are parsed into the monitor's auth form here.
 *
 * The document is only read — nothing is executed and nothing is stored.
 */

export type AuthImportFormat = 'json' | 'yaml' | 'properties';

export interface AuthImportResult {
  auth: KafkaAuth;
  /** Present when the document also names the brokers. */
  bootstrapServers: string | null;
  format: AuthImportFormat;
  warnings: string[];
}

const PROTOCOLS = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'] as const;
const MECHANISMS = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI', 'OAUTHBEARER'] as const;

/** Namespaces librdkafka's admin client actually understands. */
const EXTRA_ALLOW: RegExp[] = [
  /^ssl\./, /^sasl\./, /^security\./, /^socket\./, /^broker\./, /^metadata\./,
  /^reconnect\./, /^api\.version\./, /^enable\.ssl\./, /^topic\.metadata\./,
  /^client\.(id|dns\.lookup)$/, /^connections\.max\.idle\.ms$/, /^log\.level$/,
];

/** Java-only keys that live in the same namespaces but would be rejected. */
const EXTRA_DENY: RegExp[] = [
  /^ssl\.truststore\./, /^ssl\.endpoint\.identification\.algorithm$/,
  /^sasl\.jaas\.config$/, /^sasl\.login\./, /^sasl\.client\./, /^sasl\.server\./,
  /^ssl\.enabled\.protocols$/, /^ssl\.protocol$/, /^ssl\.truststore$/,
];

interface Entry {
  /** Key exactly as written in the document, for messages. */
  path: string;
  /** Dotted, lower-case form: `securityProtocol` and `SECURITY_PROTOCOL` both land on `security.protocol`. */
  canon: string;
  value: string;
  raw: unknown;
}

export function importKafkaAuth(source: string): AuthImportResult {
  const text = source.trim();
  if (!text) throw new Error('nothing to import');

  const warnings: string[] = [];
  const format = detectFormat(text);
  const entries = format === 'properties'
    ? parseProperties(text)
    : parseDocument(text, format, warnings);

  if (entries.length === 0) throw new Error('no configuration keys found in the document');

  const used = new Set<Entry>();
  const auth: KafkaAuth = {
    securityProtocol: 'PLAINTEXT', saslMechanism: 'PLAIN',
    username: '', password: '', sslCaLocation: '', sslSkipVerify: false, extra: {},
  };

  // ─── Brokers ──────────────────────────────────────────────────────────────
  const brokerEntry = take(entries, used, [
    'bootstrap.servers', 'metadata.broker.list', 'broker.list', 'brokers', 'bootstrap.server', 'servers',
  ]);
  const bootstrapServers = brokerEntry ? normalizeBrokers(brokerEntry.value, warnings) : null;

  // ─── SASL credentials ─────────────────────────────────────────────────────
  const mechEntry = take(entries, used, ['sasl.mechanism', 'sasl.mechanisms']);
  const jaas = take(entries, used, ['sasl.jaas.config']);
  const jaasParts = jaas ? parseJaas(jaas.value, warnings, Boolean(mechEntry)) : null;

  const userEntry = take(entries, used, ['sasl.username', 'sasl.plain.username', 'username', 'user']);
  const passEntry = take(entries, used, ['sasl.password', 'sasl.plain.password', 'password', 'pass'],
    ['ssl.key.password', 'ssl.keystore.password', 'ssl.truststore.password', 'keystore.password', 'truststore.password']);
  auth.username = userEntry?.value ?? jaasParts?.username ?? '';
  auth.password = passEntry?.value ?? jaasParts?.password ?? '';

  const mech = normalizeMechanism(mechEntry?.value ?? jaasParts?.mechanism ?? '', warnings, Boolean(mechEntry));
  if (mech) auth.saslMechanism = mech;

  // ─── TLS ──────────────────────────────────────────────────────────────────
  const caEntry = take(entries, used, [
    'ssl.ca.location', 'ssl.ca.file', 'ssl.cafile', 'ssl.ca.path', 'ssl.ca.cert', 'ssl.ca', 'ca.location', 'ca.file', 'ca',
  ]);
  auth.sslCaLocation = caEntry?.value ?? '';

  const verifyEntry = take(entries, used, [
    'enable.ssl.certificate.verification', 'ssl.reject.unauthorized', 'reject.unauthorized',
  ]);
  const skipEntry = take(entries, used, ['ssl.skip.verify', 'skip.verify', 'insecure.skip.verify', 'insecure']);
  if (verifyEntry) auth.sslSkipVerify = !truthy(verifyEntry.value);
  if (skipEntry) auth.sslSkipVerify = truthy(skipEntry.value);

  // Java says "trust any hostname" by blanking the identification algorithm.
  const idAlgo = entries.find((e) => !used.has(e) && e.canon.endsWith('ssl.endpoint.identification.algorithm'));
  if (idAlgo && idAlgo.value.trim() === '') {
    auth.sslSkipVerify = true;
    warnings.push('ssl.endpoint.identification.algorithm was empty — imported as "skip certificate verification"');
  }

  const truststore = entries.find((e) => e.canon.endsWith('ssl.truststore.location'));
  if (truststore && !auth.sslCaLocation) {
    warnings.push(`${truststore.path}: a JKS truststore cannot be read by this client — export the CA to PEM and set the CA path`);
  }

  // ─── Security protocol ────────────────────────────────────────────────────
  // `ssl.protocol` is Java's TLS version, not a Kafka security protocol.
  const protoEntry = take(entries, used, ['security.protocol', 'protocol'],
    ['ssl.protocol', 'ssl.enabled.protocols', 'tls.protocol']);
  const sslFlag = take(entries, used, ['ssl.enabled', 'ssl', 'tls.enabled', 'tls']);
  const hasSasl = Boolean(auth.username || auth.password || mechEntry || jaas);
  const hasTls = Boolean(auth.sslCaLocation) || (sslFlag ? truthy(sslFlag.value) : false);

  if (protoEntry) {
    const p = protoEntry.value.trim().toUpperCase().replace(/[-\s]/g, '_');
    if ((PROTOCOLS as readonly string[]).includes(p)) {
      auth.securityProtocol = p as KafkaAuth['securityProtocol'];
    } else {
      warnings.push(`${protoEntry.path}: "${protoEntry.value}" is not a security protocol — left as ${inferProtocol(hasSasl, hasTls)}`);
      auth.securityProtocol = inferProtocol(hasSasl, hasTls);
    }
  } else {
    auth.securityProtocol = inferProtocol(hasSasl, hasTls);
    if (auth.securityProtocol !== 'PLAINTEXT') {
      warnings.push(`the document does not name a security protocol — inferred ${auth.securityProtocol} from the keys it does have`);
    }
  }

  const usesSasl = auth.securityProtocol === 'SASL_SSL' || auth.securityProtocol === 'SASL_PLAINTEXT';
  if (!usesSasl && (auth.username || auth.password)) {
    warnings.push(`credentials are present but the protocol is ${auth.securityProtocol} — they will not be sent`);
  }
  if (usesSasl && !auth.username && auth.saslMechanism !== 'GSSAPI' && auth.saslMechanism !== 'OAUTHBEARER') {
    warnings.push(`${auth.securityProtocol} needs a username — the document did not have one`);
  }

  // ─── Everything else ──────────────────────────────────────────────────────
  const ignored: string[] = [];
  for (const e of entries) {
    if (used.has(e)) continue;
    const key = librdkafkaTail(e.canon);
    if (!key || EXTRA_DENY.some((r) => r.test(key))) {
      ignored.push(e.path);
      continue;
    }
    auth.extra[key] = e.value;
  }
  if (ignored.length) {
    const shown = ignored.slice(0, 8).join(', ');
    warnings.push(`ignored ${ignored.length} key${ignored.length === 1 ? '' : 's'} the monitor's client does not use: ${shown}${ignored.length > 8 ? ', …' : ''}`);
  }

  return { auth, bootstrapServers, format, warnings };
}

/**
 * Return the part of a key that is a librdkafka property, or null. Nested
 * documents put the real property behind a wrapper (`kafka.socket.timeout.ms`),
 * so every tail is tried, longest first.
 */
function librdkafkaTail(canon: string): string | null {
  const parts = canon.split('.');
  for (let i = 0; i < parts.length; i++) {
    const tail = parts.slice(i).join('.');
    if (EXTRA_ALLOW.some((r) => r.test(tail))) return tail;
  }
  return null;
}

function inferProtocol(hasSasl: boolean, hasTls: boolean): KafkaAuth['securityProtocol'] {
  if (hasSasl && hasTls) return 'SASL_SSL';
  if (hasSasl) return 'SASL_PLAINTEXT';
  if (hasTls) return 'SSL';
  return 'PLAINTEXT';
}

// ─── Parsing ─────────────────────────────────────────────────────────────────

function detectFormat(text: string): AuthImportFormat {
  if (text.startsWith('{') || text.startsWith('[')) return 'json';
  let props = 0;
  let yamlish = 0;
  for (const line of text.split(/\r?\n/)) {
    const l = line.trim();
    if (!l || l.startsWith('#') || l.startsWith('!') || l.startsWith('//')) continue;
    if (/^[A-Za-z_][\w.\-]*\s*=/.test(l)) props++;
    else if (/^-?\s*["']?[\w.\-]+["']?\s*:(\s|$)/.test(l)) yamlish++;
  }
  return props > 0 && props >= yamlish ? 'properties' : 'yaml';
}

/** Java/librdkafka properties, including backslash line continuations. */
function parseProperties(text: string): Entry[] {
  const out: Entry[] = [];
  const lines = text.replace(/\\\r?\n\s*/g, ' ').split(/\r?\n/);
  for (const line of lines) {
    const l = line.trim();
    if (!l || l.startsWith('#') || l.startsWith('!') || l.startsWith('//')) continue;
    const eq = l.indexOf('=');
    const colon = l.indexOf(':');
    const at = eq >= 0 && (colon < 0 || eq < colon) ? eq : colon;
    if (at <= 0) continue;
    const key = l.slice(0, at).trim().replace(/^export\s+/, '');
    const value = unquote(l.slice(at + 1).trim());
    if (!key) continue;
    out.push({ path: key, canon: canonKey(key), value, raw: value });
  }
  return out;
}

function parseDocument(text: string, format: AuthImportFormat, warnings: string[]): Entry[] {
  let doc: unknown;
  try {
    doc = yamlParse(text);
  } catch (err) {
    throw new Error(`not parseable ${format === 'json' ? 'JSON' : 'YAML'}: ${(err as Error).message}`);
  }
  if (doc === null || typeof doc !== 'object') {
    throw new Error('expected an object of configuration keys');
  }
  const out: Entry[] = [];
  flatten(doc, [], out, warnings);
  return out;
}

function flatten(node: unknown, path: string[], out: Entry[], warnings: string[]): void {
  if (node === null || node === undefined) return;
  if (out.length > 500) return;

  if (Array.isArray(node)) {
    // `brokers: [a, b]` is the common case; a list of objects is a listener
    // definition or similar, which the form has no home for.
    if (node.every(isScalar)) push(out, path, node.map((v) => String(v)).join(','), node);
    else if (path.length) warnings.push(`ignored ${path.join('.')}: a list of objects`);
    return;
  }
  if (typeof node === 'object') {
    for (const [k, v] of Object.entries(node as Record<string, unknown>)) {
      flatten(v, [...path, k], out, warnings);
    }
    // `ssl: {}` / `sasl: {}` still say TLS or SASL is in play.
    if (Object.keys(node as object).length === 0 && path.length) push(out, path, 'true', true);
    return;
  }
  if (isScalar(node)) push(out, path, String(node), node);
}

function isScalar(v: unknown): boolean {
  return typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean';
}

function push(out: Entry[], path: string[], value: string, raw: unknown): void {
  if (path.length === 0) return;
  const p = path.join('.');
  out.push({ path: p, canon: canonKey(p), value, raw });
}

/**
 * `securityProtocol`, `SECURITY_PROTOCOL`, `security-protocol` and
 * `security.protocol` are the same setting written four ways.
 */
function canonKey(raw: string): string {
  return raw
    .replace(/([a-z0-9])([A-Z])/g, '$1.$2')
    .replace(/[\s_\-]+/g, '.')
    .toLowerCase()
    .replace(/\.+/g, '.')
    .replace(/^\.|\.$/g, '');
}

/**
 * Match on the tail of the key so a nested document works without knowing its
 * wrapper: `kafka.sasl.username` and `spec.client.sasl.username` both hit.
 */
function take(entries: Entry[], used: Set<Entry>, aliases: string[], deny: string[] = []): Entry | null {
  for (const alias of aliases) {
    for (const e of entries) {
      if (used.has(e)) continue;
      if (e.canon !== alias && !e.canon.endsWith(`.${alias}`)) continue;
      if (deny.some((d) => e.canon === d || e.canon.endsWith(`.${d}`))) continue;
      used.add(e);
      return e;
    }
  }
  return null;
}

function unquote(v: string): string {
  const m = /^(['"])([\s\S]*)\1$/.exec(v);
  return m ? m[2]! : v;
}

function truthy(v: string): boolean {
  return /^(1|true|yes|on|enabled)$/i.test(v.trim());
}

function normalizeBrokers(value: string, warnings: string[]): string | null {
  const hosts = value.split(',').map((h) => h.trim()).filter(Boolean).map((h) => {
    const stripped = h.replace(/^[A-Za-z_]+:\/\//, '');
    if (stripped !== h) warnings.push(`dropped the listener prefix from "${h}"`);
    return stripped;
  });
  return hosts.length ? hosts.join(',') : null;
}

function normalizeMechanism(
  value: string, warnings: string[], explicit: boolean,
): KafkaAuth['saslMechanism'] | null {
  const v = value.trim().toUpperCase().replace(/_/g, '-');
  if (!v) return null;
  if ((MECHANISMS as readonly string[]).includes(v)) return v as KafkaAuth['saslMechanism'];
  if (v === 'SCRAM-SHA512') return 'SCRAM-SHA-512';
  if (v === 'SCRAM-SHA256') return 'SCRAM-SHA-256';
  if (explicit) warnings.push(`"${value}" is not a SASL mechanism this client offers — left as PLAIN`);
  return null;
}

/**
 * Pull the credentials out of a Java JAAS line:
 *   org.apache.kafka.common.security.scram.ScramLoginModule required
 *     username="svc" password="s3cr3t";
 */
function parseJaas(
  value: string, warnings: string[], mechanismKnown: boolean,
): { username: string; password: string; mechanism: string } {
  const pick = (name: string): string => {
    const m = new RegExp(`${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s;]+))`, 'i').exec(value);
    return m ? (m[1] ?? m[2] ?? m[3] ?? '') : '';
  };
  let mechanism = '';
  if (/PlainLoginModule/i.test(value)) mechanism = 'PLAIN';
  else if (/ScramLoginModule/i.test(value)) {
    mechanism = 'SCRAM-SHA-512';
    if (!mechanismKnown) {
      warnings.push('the JAAS line does not say SHA-256 or SHA-512 — assumed SCRAM-SHA-512, change it if the broker says otherwise');
    }
  } else if (/OAuthBearerLoginModule/i.test(value)) mechanism = 'OAUTHBEARER';
  else if (/Krb5LoginModule/i.test(value)) mechanism = 'GSSAPI';
  return { username: pick('username'), password: pick('password'), mechanism };
}
