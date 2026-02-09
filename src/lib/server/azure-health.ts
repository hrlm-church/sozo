import crypto from "crypto";
import net from "net";
import { getServerEnv, looksConfigured } from "@/lib/server/env";

export type ServiceHealthStatus =
  | "healthy"
  | "degraded"
  | "not_configured"
  | "unhealthy";

export interface ServiceHealth {
  service: "sql" | "search" | "storage" | "serviceBus" | "openAi";
  status: ServiceHealthStatus;
  detail: string;
  latencyMs?: number;
}

export interface HealthPayload {
  ok: boolean;
  timestamp: string;
  environment: {
    resourceGroup: string;
    location: string;
  };
  services: ServiceHealth[];
}

const timed = async <T>(fn: () => Promise<T>) => {
  const started = Date.now();
  try {
    const value = await fn();
    return {
      ok: true as const,
      value,
      latencyMs: Date.now() - started,
    };
  } catch (error) {
    return {
      ok: false as const,
      error,
      latencyMs: Date.now() - started,
    };
  }
};

const checkSql = async (): Promise<ServiceHealth> => {
  const env = getServerEnv();
  const host = env.sqlHost;

  const result = await timed(
    () =>
      new Promise<void>((resolve, reject) => {
        const socket = net.connect(1433, host);

        const done = (error?: Error) => {
          socket.removeAllListeners();
          socket.destroy();
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        };

        socket.setTimeout(2500, () => done(new Error("SQL connect timeout")));
        socket.once("connect", () => done());
        socket.once("error", (error) => done(error));
      }),
  );

  if (!result.ok) {
    return {
      service: "sql",
      status: "unhealthy",
      detail: `TCP probe failed for ${host}:1433`,
      latencyMs: result.latencyMs,
    };
  }

  return {
    service: "sql",
    status: "healthy",
    detail: `TCP probe connected to ${host}:1433`,
    latencyMs: result.latencyMs,
  };
};

const checkSearch = async (): Promise<ServiceHealth> => {
  const env = getServerEnv();
  if (!looksConfigured(env.searchAdminKey)) {
    return {
      service: "search",
      status: "not_configured",
      detail: "SOZO_SEARCH_ADMIN_KEY is missing",
    };
  }

  const endpoint = `https://${env.searchServiceName}.search.windows.net/indexes?api-version=2024-07-01`;
  const result = await timed(() =>
    fetch(endpoint, {
      headers: {
        "api-key": env.searchAdminKey!,
      },
      cache: "no-store",
    }),
  );

  if (!result.ok) {
    return {
      service: "search",
      status: "unhealthy",
      detail: "Search request failed",
      latencyMs: result.latencyMs,
    };
  }

  if (!result.value.ok) {
    return {
      service: "search",
      status: "unhealthy",
      detail: `Search API returned ${result.value.status}`,
      latencyMs: result.latencyMs,
    };
  }

  return {
    service: "search",
    status: "healthy",
    detail: "Search admin API responded",
    latencyMs: result.latencyMs,
  };
};

const buildStorageAuthHeader = (
  accountName: string,
  accountKey: string,
  xMsDate: string,
): string => {
  const stringToSign = [
    "GET",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    `x-ms-date:${xMsDate}\nx-ms-version:2023-11-03`,
    `/${accountName}/\ncomp:list`,
  ].join("\n");

  const hmac = crypto.createHmac("sha256", Buffer.from(accountKey, "base64"));
  hmac.update(stringToSign, "utf8");
  return `SharedKey ${accountName}:${hmac.digest("base64")}`;
};

const checkStorage = async (): Promise<ServiceHealth> => {
  const env = getServerEnv();
  if (!looksConfigured(env.storageAccountKey)) {
    return {
      service: "storage",
      status: "not_configured",
      detail: "SOZO_STORAGE_ACCOUNT_KEY (or AZURE_STORAGE_KEY) is missing",
    };
  }

  const xMsDate = new Date().toUTCString();
  const endpoint = `https://${env.storageAccount}.blob.core.windows.net/?comp=list`;
  const result = await timed(() =>
    fetch(endpoint, {
      method: "GET",
      headers: {
        "x-ms-date": xMsDate,
        "x-ms-version": "2023-11-03",
        Authorization: buildStorageAuthHeader(
          env.storageAccount,
          env.storageAccountKey!,
          xMsDate,
        ),
      },
      cache: "no-store",
    }),
  );

  if (!result.ok) {
    return {
      service: "storage",
      status: "unhealthy",
      detail: "Storage request failed",
      latencyMs: result.latencyMs,
    };
  }

  if (!result.value.ok) {
    return {
      service: "storage",
      status: "unhealthy",
      detail: `Storage API returned ${result.value.status}`,
      latencyMs: result.latencyMs,
    };
  }

  return {
    service: "storage",
    status: "healthy",
    detail: "Storage blob API responded",
    latencyMs: result.latencyMs,
  };
};

const buildServiceBusSasToken = (
  namespace: string,
  policyName: string,
  policyKey: string,
): string => {
  const encodedUri = encodeURIComponent(
    `https://${namespace}.servicebus.windows.net/`,
  );
  const expires = Math.floor(Date.now() / 1000) + 60 * 15;
  const toSign = `${encodedUri}\n${expires}`;

  const signature = encodeURIComponent(
    crypto
      .createHmac("sha256", policyKey)
      .update(toSign, "utf8")
      .digest("base64"),
  );

  return `SharedAccessSignature sr=${encodedUri}&sig=${signature}&se=${expires}&skn=${policyName}`;
};

const checkServiceBus = async (): Promise<ServiceHealth> => {
  const env = getServerEnv();
  if (!looksConfigured(env.serviceBusPolicyName) || !looksConfigured(env.serviceBusPolicyKey)) {
    return {
      service: "serviceBus",
      status: "not_configured",
      detail: "SOZO_SERVICEBUS_POLICY_NAME and SOZO_SERVICEBUS_POLICY_KEY are required",
    };
  }

  const endpoint = `https://${env.serviceBusNamespace}.servicebus.windows.net/?api-version=2021-05`;
  const token = buildServiceBusSasToken(
    env.serviceBusNamespace,
    env.serviceBusPolicyName!,
    env.serviceBusPolicyKey!,
  );

  const result = await timed(() =>
    fetch(endpoint, {
      headers: {
        Authorization: token,
      },
      cache: "no-store",
    }),
  );

  if (!result.ok) {
    return {
      service: "serviceBus",
      status: "unhealthy",
      detail: "Service Bus request failed",
      latencyMs: result.latencyMs,
    };
  }

  if (!result.value.ok) {
    return {
      service: "serviceBus",
      status: "unhealthy",
      detail: `Service Bus API returned ${result.value.status}`,
      latencyMs: result.latencyMs,
    };
  }

  return {
    service: "serviceBus",
    status: "healthy",
    detail: "Service Bus namespace responded",
    latencyMs: result.latencyMs,
  };
};

const checkOpenAi = async (): Promise<ServiceHealth> => {
  const env = getServerEnv();
  if (
    !looksConfigured(env.openAiEndpoint) ||
    !looksConfigured(env.openAiKey) ||
    !looksConfigured(env.openAiDeployment)
  ) {
    return {
      service: "openAi",
      status: "not_configured",
      detail:
        "SOZO_OPENAI_ENDPOINT, SOZO_OPENAI_API_KEY, and SOZO_OPENAI_CHAT_DEPLOYMENT are required",
    };
  }

  const endpoint = env.openAiEndpoint!.replace(/\/$/, "");
  const url = `${endpoint}/openai/deployments/${encodeURIComponent(
    env.openAiDeployment!,
  )}/chat/completions?api-version=${encodeURIComponent(env.openAiApiVersion)}`;

  const result = await timed(() =>
    fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": env.openAiKey!,
      },
      body: JSON.stringify({
        messages: [{ role: "user", content: "health-check" }],
        max_tokens: 1,
        temperature: 0,
      }),
      cache: "no-store",
    }),
  );

  if (!result.ok) {
    return {
      service: "openAi",
      status: "unhealthy",
      detail: "OpenAI control-plane request failed",
      latencyMs: result.latencyMs,
    };
  }

  if (!result.value.ok) {
    return {
      service: "openAi",
      status: "unhealthy",
      detail: `OpenAI API returned ${result.value.status}`,
      latencyMs: result.latencyMs,
    };
  }

  return {
    service: "openAi",
    status: "healthy",
    detail: `Deployment ${env.openAiDeployment} accepted chat completion request`,
    latencyMs: result.latencyMs,
  };
};

export const getSystemHealth = async (): Promise<HealthPayload> => {
  const env = getServerEnv();
  const services = await Promise.all([
    checkSql(),
    checkSearch(),
    checkStorage(),
    checkServiceBus(),
    checkOpenAi(),
  ]);

  const hasFailure = services.some((service) => service.status === "unhealthy");

  return {
    ok: !hasFailure,
    timestamp: new Date().toISOString(),
    environment: {
      resourceGroup: env.azureResourceGroup,
      location: env.azureLocation,
    },
    services,
  };
};
