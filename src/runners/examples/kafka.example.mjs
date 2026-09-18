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

// Optional: `options` puts the run's conditions in the script itself, so the
// profile form only needs the broker connection. Every key is optional and
// overrides the form for this run; an unknown key or bad value is logged and
// ignored. `thresholds` are added to the profile's own.
//
// This one is a volume test: push 2 GB, then give the consumer group up to
// 2 minutes to work through the backlog, and fail the run if it can't.
export const options = {
  targetRate: 5000,          // msg/s
  durationSec: 1800,         // upper bound — maxMb normally ends it first
  maxMb: 2000,               // stop after 2000 MB (key + value) acked; 0 = unlimited
  producers: 2,
  consumerGroup: 'orders-service', // the group under test; '' = every group on the topic
  monitorLag: true,
  drainTimeoutSec: 120,      // after producing, wait for lag 0 (measures drain time)
  thresholds: [
    'lag_max < 200000',      // backlog never exceeds 200k messages
    'lag_growth <= 50',      // consumers keep (roughly) up while load runs
    'lag_drain_s < 60',      // and clear the backlog within a minute afterwards
    'total_mb >= 2000',      // the full volume actually landed
    'success_rate > 99.9',
  ],
};

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
