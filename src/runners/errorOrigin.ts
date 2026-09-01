/**
 * Who actually produced a failed response.
 *
 * A load test aimed at a service usually goes through something first — Azure
 * API Management, an APISIX/API7 gateway, an ingress, a CDN. When the run turns
 * red it matters enormously whether the 503 came from the service under test or
 * from the hop in front of it, and the raw status code never says.
 *
 * This is a heuristic over the response headers, the body shape and the address
 * that answered. It reports its evidence so the reader can disagree with it,
 * and says "unknown" rather than guessing when the signals are absent.
 */

import type { ErrorOrigin } from "../shared/types.ts";

export interface OriginInput {
  status?: number;
  /** Already redacted — evidence must never carry a credential. */
  headers: Record<string, string>;
  body?: string;
  remoteIp?: string;
  remotePort?: number;
  proto?: string;
  url?: string;
}

interface Signature {
  name: string;
  /** Header whose presence alone identifies the hop. */
  header?: RegExp;
  /** Header value pattern, tested against `Server` unless `on` says otherwise. */
  on?: string;
  value?: RegExp;
}

/** Hops that sit in front of a service and can answer in its place. */
const GATEWAYS: Signature[] = [
  { name: "Azure API Management", on: "server", value: /azure[-_ ]?api[-_ ]?management|APIM/i },
  { name: "Azure API Management", header: /^ocp-apim-|^apim-request-id$/i },
  { name: "Azure Application Gateway", on: "server", value: /Microsoft-Azure-Application-Gateway/i },
  { name: "Azure Front Door", header: /^x-azure-ref$|^x-fd-|^x-msedge-ref$/i },
  { name: "APISIX / API7", on: "server", value: /apisix/i },
  { name: "APISIX / API7", header: /^x-apisix-/i },
  { name: "Kong", on: "server", value: /kong/i },
  { name: "Kong", header: /^x-kong-/i },
  { name: "Envoy", on: "server", value: /^envoy$/i },
  { name: "Envoy", header: /^x-envoy-/i },
  { name: "Istio", on: "server", value: /istio-envoy/i },
  { name: "Cloudflare", header: /^cf-ray$/i },
  { name: "Cloudflare", on: "server", value: /cloudflare/i },
  { name: "Amazon CloudFront", header: /^x-amz-cf-id$/i },
  { name: "AWS load balancer", on: "server", value: /awselb|awsalb/i },
  { name: "Fastly", header: /^fastly-|^x-served-by$/i },
  { name: "Akamai", header: /^x-akamai-/i },
  { name: "Akamai", on: "server", value: /AkamaiGHost/i },
  { name: "Traefik", on: "server", value: /traefik/i },
  { name: "HAProxy", on: "server", value: /haproxy/i },
  { name: "nginx", on: "server", value: /nginx|openresty/i },
  { name: "Apache httpd", on: "server", value: /^apache/i },
];

/**
 * Headers a proxy only sets once the backend has answered. Their presence means
 * the request did reach the service, so the failure is the service's own.
 */
const UPSTREAM_REACHED = [
  /^x-envoy-upstream-service-time$/i,
  /^x-kong-upstream-latency$/i,
  /^x-apisix-upstream-status$/i,
  /^x-upstream-status$/i,
  /^x-backend-server$/i,
  /^x-application-context$/i,
];

/** Application servers — if one of these answered, it is the service talking. */
const APP_SERVERS = /kestrel|express|gunicorn|uvicorn|hypercorn|tomcat|jetty|werkzeug|puma|unicorn|node\.js|fastify|iis|jboss|wildfly|undertow|netty/i;

/** Error pages a gateway writes itself, rather than relaying one. */
const GATEWAY_BODIES: Array<{ name: string; test: RegExp }> = [
  { name: "Azure Application Gateway", test: /Microsoft-Azure-Application-Gateway/i },
  { name: "nginx", test: /<center>\s*(nginx|openresty)/i },
  { name: "Envoy", test: /upstream connect error|no healthy upstream|upstream request timeout/i },
  { name: "APISIX / API7", test: /"error_msg"\s*:/i },
  { name: "Amazon CloudFront", test: /The request could not be satisfied/i },
  { name: "Akamai", test: /Reference&#32;&#35;|akamaized/i },
];

/** Correlation ids — what you take to the gateway's own logs to find this call. */
const TRACE_HEADERS =
  /^(x-request-id|x-correlation-id|x-ms-request-id|x-ms-correlation-request-id|apim-request-id|request-id|x-amzn-requestid|x-amz-cf-id|x-azure-ref|x-msedge-ref|cf-ray|x-kong-request-id|x-b3-traceid|traceparent|x-trace-id)$/i;

const MAX_EVIDENCE = 6;

export function classifyOrigin(input: OriginInput): ErrorOrigin {
  const entries = Object.entries(input.headers ?? {});
  const find = (name: string): [string, string] | undefined =>
    entries.find(([k]) => k.toLowerCase() === name);
  const get = (name: string): string => find(name)?.[1] ?? "";
  const evidence: string[] = [];
  const note = (line: string): void => {
    if (evidence.length < MAX_EVIDENCE && !evidence.includes(line))
      evidence.push(line);
  };

  // ── Which hop is in the path at all
  let gateway: string | undefined;
  for (const sig of GATEWAYS) {
    if (sig.header) {
      const hit = entries.find(([k]) => sig.header!.test(k));
      if (hit) {
        gateway ??= sig.name;
        note(`${hit[0]}: ${hit[1]}`);
      }
      continue;
    }
    const hit = find(sig.on ?? "server");
    if (sig.value && hit && sig.value.test(hit[1])) {
      gateway ??= sig.name;
      note(`${hit[0]}: ${hit[1]}`);
    }
  }
  const via = get("via");
  if (via) {
    gateway ??= `proxy (Via: ${via})`;
    note(`Via: ${via}`);
  }

  // ── Did the request get through to the service
  const reached = entries.find(([k]) =>
    UPSTREAM_REACHED.some((re) => re.test(k)),
  );
  if (reached) note(`${reached[0]}: ${reached[1]}`);

  const server = get("server");
  const appServer = server && APP_SERVERS.test(server);
  const poweredBy = get("x-powered-by");
  if (appServer) note(`${find("server")?.[0] ?? "Server"}: ${server}`);
  if (poweredBy)
    note(`${find("x-powered-by")?.[0] ?? "X-Powered-By"}: ${poweredBy}`);

  // ── Did the hop write this error page itself
  const body = input.body ?? "";
  const gatewayBody = GATEWAY_BODIES.find((b) => b.test.test(body));
  // Azure APIM's own errors are a bare {"statusCode":…,"message":…} envelope;
  // a relayed backend error keeps the backend's own shape.
  const apimEnvelope =
    /^\s*\{\s*"statusCode"\s*:\s*\d+\s*,\s*"message"\s*:/.test(body);
  if (gatewayBody) note(`body written by ${gatewayBody.name}`);
  if (apimEnvelope) note("body is an API Management error envelope");

  const status = input.status ?? 0;
  // A gateway that never reached the backend answers 502/503/504 itself.
  const gatewayStatus = status === 502 || status === 503 || status === 504;

  let verdict: ErrorOrigin["verdict"] = "unknown";
  let by = gateway;
  if (gatewayBody) {
    verdict = "gateway";
    by = gatewayBody.name;
  } else if (apimEnvelope && gateway) {
    verdict = "gateway";
  } else if (reached || appServer || poweredBy) {
    // Something downstream of the gateway answered, so the body is the service's.
    verdict = "service";
    // `Server` may still be the proxy's; only trust it when it names an app
    // server, otherwise the X-Powered-By style signal identifies the service.
    const serviceName = appServer ? server : poweredBy;
    if (serviceName) by = serviceName;
    else if (gateway) by = undefined;
  } else if (gateway && gatewayStatus) {
    verdict = "gateway";
  } else if (gateway) {
    verdict = "unknown";
  } else if (status > 0) {
    // Nothing announced itself as a proxy — take the target at its word.
    verdict = "service";
  }

  const traceIds: Record<string, string> = {};
  for (const [k, v] of entries) if (TRACE_HEADERS.test(k)) traceIds[k] = v;

  return {
    verdict,
    ...(by ? { by } : {}),
    ...(gateway ? { gateway } : {}),
    evidence,
    ...(Object.keys(traceIds).length ? { traceIds } : {}),
    ...(input.remoteIp ? { remoteIp: input.remoteIp } : {}),
    ...(input.remotePort ? { remotePort: input.remotePort } : {}),
    ...(input.proto ? { proto: input.proto } : {}),
    ...(input.url ? { url: input.url } : {}),
  };
}
