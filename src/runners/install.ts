/**
 * One-click installation of the external tools a run depends on.
 *
 * Two rules hold this together:
 *  - Nothing here is user-supplied. Every command is a constant in INSTALLERS,
 *    executed argv-style with `shell: false`, so no dashboard input can ever
 *    reach a command line. The client picks a method *id*, not a command.
 *  - A method that cannot run unattended is never run. Package managers that
 *    need a root password would hang forever behind an HTTP request, so those
 *    come back as copy-me text with the reason attached instead.
 */

import { spawn } from "node:child_process";
import { platform as osPlatform } from "node:os";
import { getSettings } from "../store/db.ts";
import { probe } from "./k6.runner.ts";
import type {
  InstallResult,
  ToolId,
  ToolPlatform,
  ToolStatus,
  ToolsInfo,
} from "../shared/types.ts";

/** A single command in an install recipe. */
interface Step {
  argv: string[];
  /** Fed to the command's stdin — replaces a shell pipe such as `echo … | tee`. */
  stdin?: string;
}

interface MethodSpec {
  id: string;
  label: string;
  /** Package manager that must be on PATH for this method to be offered. */
  requires: string;
  /** Args that make `requires` print its version and exit 0. */
  requiresArgs?: string[];
  steps: Step[];
  /** Root is needed, so it only runs where sudo is already passwordless. */
  needsSudo?: boolean;
  note?: string;
}

interface ToolSpec {
  id: ToolId;
  label: string;
  docsUrl: string;
  /** Configured binary, so the probe matches what a run will actually spawn. */
  binPath(): string;
  /** Args that make the tool print its version — same probe the runner uses. */
  versionArgs: string[];
  methods: Partial<Record<ToolPlatform, MethodSpec[]>>;
}

/** k6's signing key, as published in the Grafana install docs. */
const K6_KEY = "C5AD17C747E3415A3642D57D77C6C491D6AC1D69";
const K6_APT_LIST = "/etc/apt/sources.list.d/k6.list";
const K6_APT_SOURCE =
  "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main\n";

const INSTALLERS: ToolSpec[] = [
  {
    id: "k6",
    label: "k6",
    docsUrl: "https://grafana.com/docs/k6/latest/set-up/install-k6/",
    binPath: () => getSettings().k6Path,
    versionArgs: ["version"],
    methods: {
      darwin: [
        {
          id: "brew",
          label: "Homebrew",
          requires: "brew",
          steps: [{ argv: ["brew", "install", "k6"] }],
        },
      ],
      win32: [
        {
          id: "winget",
          label: "winget",
          requires: "winget",
          steps: [
            {
              argv: [
                "winget",
                "install",
                "--id",
                "k6.k6",
                "-e",
                "--accept-package-agreements",
                "--accept-source-agreements",
              ],
            },
          ],
          note: "Open a new terminal (or restart the dashboard) afterwards so the updated PATH is picked up.",
        },
        {
          id: "choco",
          label: "Chocolatey",
          requires: "choco",
          steps: [{ argv: ["choco", "install", "k6", "-y"] }],
          note: "Chocolatey needs an elevated shell; start the dashboard as administrator if this fails.",
        },
      ],
      linux: [
        {
          id: "apt",
          label: "APT (Debian / Ubuntu)",
          requires: "apt-get",
          needsSudo: true,
          steps: [
            { argv: ["sudo", "gpg", "-k"] },
            {
              argv: [
                "sudo",
                "gpg",
                "--no-default-keyring",
                "--keyring",
                "/usr/share/keyrings/k6-archive-keyring.gpg",
                "--keyserver",
                "hkp://keyserver.ubuntu.com:80",
                "--recv-keys",
                K6_KEY,
              ],
            },
            { argv: ["sudo", "tee", K6_APT_LIST], stdin: K6_APT_SOURCE },
            { argv: ["sudo", "apt-get", "update"] },
            { argv: ["sudo", "apt-get", "install", "-y", "k6"] },
          ],
        },
        {
          id: "snap",
          label: "Snap",
          requires: "snap",
          requiresArgs: ["version"],
          needsSudo: true,
          steps: [{ argv: ["sudo", "snap", "install", "k6"] }],
        },
      ],
    },
  },
  {
    id: "artillery",
    label: "Artillery",
    docsUrl: "https://www.artillery.io/docs/get-started/get-artillery",
    binPath: () => getSettings().artilleryPath,
    versionArgs: ["--version"],
    methods: {
      darwin: [npmGlobalArtillery()],
      win32: [npmGlobalArtillery()],
      linux: [npmGlobalArtillery()],
    },
  },
];

function npmGlobalArtillery(): MethodSpec {
  return {
    id: "npm",
    label: "npm (global)",
    requires: "npm",
    steps: [{ argv: ["npm", "install", "-g", "artillery"] }],
    note: "Installs into npm's global prefix. If that directory is root-owned, run the command in a terminal instead.",
  };
}

export function currentPlatform(): ToolPlatform {
  const p = osPlatform();
  return p === "darwin" || p === "win32" || p === "linux" ? p : "other";
}

/** Shell rendering of a recipe, for the copy button — never executed here. */
function commandText(steps: Step[]): string {
  return steps
    .map((s) =>
      s.stdin
        ? `echo ${quote(s.stdin.trim())} | ${s.argv.map(quote).join(" ")}`
        : s.argv.map(quote).join(" "),
    )
    .join("\n");
}

function quote(s: string): string {
  return /^[A-Za-z0-9_./:=@-]+$/.test(s) ? s : `'${s.replaceAll("'", `'\\''`)}'`;
}

/** Exit status of a command run purely to see whether it exists. */
function canRun(bin: string, args: string[]): Promise<boolean> {
  return new Promise((resolve) => {
    const child = spawn(bin, args, {
      shell: false,
      stdio: ["ignore", "ignore", "ignore"],
    });
    child.on("error", () => resolve(false));
    child.on("close", (code) => resolve(code === 0));
  });
}

/** True only when sudo will not stop to ask for a password. */
function sudoIsPasswordless(): Promise<boolean> {
  return canRun("sudo", ["-n", "true"]);
}

export async function toolsInfo(): Promise<ToolsInfo> {
  const platform = currentPlatform();
  const sudoOk = platform === "linux" ? await sudoIsPasswordless() : false;
  const tools = await Promise.all(
    INSTALLERS.map((spec) => statusOf(spec, platform, sudoOk)),
  );
  return { platform, tools };
}

async function statusOf(
  spec: ToolSpec,
  platform: ToolPlatform,
  sudoOk: boolean,
): Promise<ToolStatus> {
  const bin = spec.binPath();
  const probed = await probe(bin, spec.versionArgs);
  const specs = spec.methods[platform] ?? [];
  const methods = await Promise.all(
    specs.map(async (m) => {
      const present = await canRun(m.requires, m.requiresArgs ?? ["--version"]);
      const reason = !present
        ? `${m.requires} is not on PATH`
        : m.needsSudo && !sudoOk
          ? "needs root — sudo would ask for a password, which this button cannot answer"
          : undefined;
      return {
        id: m.id,
        label: m.label,
        command: commandText(m.steps),
        runnable: present && (!m.needsSudo || sudoOk),
        ...(reason ? { reason } : {}),
        ...(m.note ? { note: m.note } : {}),
        needsSudo: Boolean(m.needsSudo),
      };
    }),
  );
  return {
    id: spec.id,
    label: spec.label,
    binPath: bin,
    docsUrl: spec.docsUrl,
    available: probed.available,
    detail: probed.detail,
    methods,
  };
}

/** Output kept from an install, newest lines win when a build log runs long. */
const MAX_OUTPUT_LINES = 400;
/** A cold `brew install` can genuinely take minutes; a hung one must not hang forever. */
const INSTALL_TIMEOUT_MS = 15 * 60 * 1000;

let installing: string | null = null;

export async function installTool(
  toolId: string,
  methodId: string,
): Promise<InstallResult> {
  const spec = INSTALLERS.find((s) => s.id === toolId);
  if (!spec) throw new Error(`unknown tool: ${toolId}`);
  const platform = currentPlatform();
  const method = (spec.methods[platform] ?? []).find((m) => m.id === methodId);
  if (!method)
    throw new Error(`no ${methodId} installer for ${toolId} on ${platform}`);

  const present = await canRun(method.requires, method.requiresArgs ?? ["--version"]);
  if (!present) throw new Error(`${method.requires} is not on PATH`);
  if (method.needsSudo && !(await sudoIsPasswordless()))
    throw new Error(
      "sudo would ask for a password — run the command in a terminal instead",
    );

  // Two package managers writing the same prefix at once is a good way to end
  // up with neither tool installed.
  if (installing)
    throw new Error(`an install is already running (${installing})`);
  installing = `${toolId}/${methodId}`;

  const output: string[] = [];
  const push = (line: string): void => {
    output.push(line);
    if (output.length > MAX_OUTPUT_LINES) output.shift();
  };

  let code: number | null = 0;
  try {
    for (const step of method.steps) {
      push(`$ ${commandText([step])}`);
      code = await runStep(step, push);
      if (code !== 0) break;
    }
  } finally {
    installing = null;
  }

  const sudoOk = platform === "linux" ? await sudoIsPasswordless() : false;
  const status = await statusOf(spec, platform, sudoOk);
  // The tool answering `version` is the only proof that matters — a package
  // manager can exit 0 and still leave the binary off this process's PATH.
  const ok = code === 0 && status.available;
  if (code === 0 && !status.available)
    push(
      `${spec.binPath()} is still not runnable from the dashboard — open a new terminal so PATH is refreshed, restart the dashboard, or set the full path in Settings.`,
    );
  return { ok, code, output, status };
}

function runStep(step: Step, push: (line: string) => void): Promise<number | null> {
  return new Promise((resolve) => {
    const [bin, ...args] = step.argv;
    const child = spawn(bin, args, {
      shell: false,
      stdio: [step.stdin ? "pipe" : "ignore", "pipe", "pipe"],
    });
    const timer = setTimeout(() => {
      push(`timed out after ${INSTALL_TIMEOUT_MS / 60000} minutes — killed`);
      child.kill("SIGKILL");
    }, INSTALL_TIMEOUT_MS);

    if (step.stdin) child.stdin?.end(step.stdin);
    const feed = (b: Buffer): void => {
      for (const line of b.toString("utf8").split("\n")) {
        const trimmed = line.trimEnd();
        if (trimmed) push(trimmed);
      }
    };
    child.stdout?.on("data", feed);
    child.stderr?.on("data", feed);
    child.on("error", (err) => {
      clearTimeout(timer);
      push(`${bin}: ${err.message}`);
      resolve(null);
    });
    child.on("close", (c) => {
      clearTimeout(timer);
      resolve(c);
    });
  });
}
