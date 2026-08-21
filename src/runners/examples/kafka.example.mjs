// Custom Kafka message generator.
//
// WARNING: unlike the k6 and Artillery scripts — which run inside their own
// tool's sandboxed runtime in a separate process — this module is imported into
// the dashboard's Node process and runs with full Node privileges. Only load
// code you would run yourself.
//
// Export `generate`. It is called once per message and must stay cheap: at a
// target rate of 20k msg/s it runs 20k times a second.
//
// Return: { value, key?, headers? }
//   value   string | Buffer | object (objects are JSON-stringified)
//   key     string | null
//   headers Record<string, string>   (the dashboard adds its own timing headers)

let userIds = null;

/** Called once before the run. Optional. */
export function setup() {
  userIds = Array.from({ length: 1000 }, (_, i) => `user-${i}`);
}

/**
 * @param {{ seq: number, ts: number, producer: number }} ctx
 */
export function generate(ctx) {
  const userId = userIds[ctx.seq % userIds.length];
  return {
    key: userId,
    value: {
      eventId: `evt-${ctx.seq}`,
      userId,
      type: ctx.seq % 10 === 0 ? 'purchase' : 'view',
      amount: ctx.seq % 10 === 0 ? Math.round(Math.random() * 10000) / 100 : undefined,
      emittedAt: ctx.ts,
    },
    headers: { 'x-event-source': 'loadtest-dashboard' },
  };
}
