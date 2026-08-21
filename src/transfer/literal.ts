import type * as ESTree from 'acorn';

/**
 * Evaluate a *static* AST node to a JavaScript value.
 *
 * Deliberately not `eval` / `new Function` / `vm`: importing a script must never
 * execute it. Anything that is not a plain literal, array, object, or a simple
 * unary/template form is refused, and the caller reports it as unmapped.
 */

export const UNRESOLVED = Symbol('unresolved');
export type Static = string | number | boolean | null | Static[] | { [k: string]: Static };

export function staticValue(node: ESTree.AnyNode | null | undefined): Static | typeof UNRESOLVED {
  if (!node) return UNRESOLVED;
  switch (node.type) {
    case 'Literal': {
      const v = (node as ESTree.Literal).value;
      if (v === null) return null;
      if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
      return UNRESOLVED; // RegExp / BigInt
    }
    case 'TemplateLiteral': {
      const t = node as ESTree.TemplateLiteral;
      // Only a template with no interpolation is static.
      if (t.expressions.length > 0) return UNRESOLVED;
      return t.quasis.map((q) => q.value.cooked ?? '').join('');
    }
    case 'UnaryExpression': {
      const u = node as ESTree.UnaryExpression;
      const inner = staticValue(u.argument);
      if (inner === UNRESOLVED) return UNRESOLVED;
      if (u.operator === '-' && typeof inner === 'number') return -inner;
      if (u.operator === '+' && typeof inner === 'number') return inner;
      if (u.operator === '!') return !inner;
      return UNRESOLVED;
    }
    case 'ArrayExpression': {
      const out: Static[] = [];
      for (const el of (node as ESTree.ArrayExpression).elements) {
        if (!el || el.type === 'SpreadElement') return UNRESOLVED;
        const v = staticValue(el);
        if (v === UNRESOLVED) return UNRESOLVED;
        out.push(v);
      }
      return out;
    }
    case 'ObjectExpression': {
      const out: { [k: string]: Static } = {};
      for (const prop of (node as ESTree.ObjectExpression).properties) {
        if (prop.type !== 'Property') return UNRESOLVED;
        const key = propertyName(prop);
        if (key === null) return UNRESOLVED;
        const v = staticValue(prop.value as ESTree.AnyNode);
        if (v === UNRESOLVED) return UNRESOLVED;
        out[key] = v;
      }
      return out;
    }
    default:
      return UNRESOLVED;
  }
}

export function propertyName(prop: ESTree.Property): string | null {
  if (prop.computed) return null;
  if (prop.key.type === 'Identifier') return prop.key.name;
  if (prop.key.type === 'Literal') return String((prop.key as ESTree.Literal).value);
  return null;
}

export function asRecord(v: Static | typeof UNRESOLVED): Record<string, Static> | null {
  return v !== UNRESOLVED && v !== null && typeof v === 'object' && !Array.isArray(v)
    ? (v as Record<string, Static>)
    : null;
}

export function asArray(v: Static | typeof UNRESOLVED): Static[] | null {
  return v !== UNRESOLVED && Array.isArray(v) ? v : null;
}

export function asString(v: Static | typeof UNRESOLVED | undefined): string | null {
  return typeof v === 'string' ? v : null;
}

export function asNumber(v: Static | typeof UNRESOLVED | undefined): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

export function asBool(v: Static | typeof UNRESOLVED | undefined): boolean | null {
  return typeof v === 'boolean' ? v : null;
}

/** "30s" / "1m30s" / 45 → seconds. k6 and Artillery both accept both forms. */
export function toSeconds(v: Static | typeof UNRESOLVED | undefined): number | null {
  if (typeof v === 'number') return Math.round(v);
  if (typeof v !== 'string') return null;
  const m = /^(?:(\d+)h)?(?:(\d+)m(?!s))?(?:(\d+(?:\.\d+)?)s)?$/.exec(v.trim());
  if (m && (m[1] || m[2] || m[3])) {
    return Math.round(Number(m[1] ?? 0) * 3600 + Number(m[2] ?? 0) * 60 + Number(m[3] ?? 0));
  }
  const plain = Number(v);
  return Number.isFinite(plain) ? Math.round(plain) : null;
}
