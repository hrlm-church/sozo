const required = [
  "SOZO_AZURE_RESOURCE_GROUP",
  "SOZO_AZURE_LOCATION",
  "SOZO_STORAGE_ACCOUNT",
  "SOZO_SERVICEBUS_NAMESPACE",
  "SOZO_SQL_HOST",
  "SOZO_SQL_DB",
  "SOZO_SEARCH_SERVICE_NAME",
] as const;

export interface SozoServerEnv {
  azureResourceGroup: string;
  azureLocation: string;
  storageAccount: string;
  storageAccountKey?: string;
  serviceBusNamespace: string;
  serviceBusPolicyName?: string;
  serviceBusPolicyKey?: string;
  sqlHost: string;
  sqlDb: string;
  sqlUser?: string;
  sqlPassword?: string;
  searchServiceName: string;
  searchAdminKey?: string;
  searchIndexName?: string;
  openAiEndpoint?: string;
  openAiKey?: string;
  openAiDeployment?: string;
  openAiApiVersion: string;
}

const value = (key: string) => {
  const raw = process.env[key];
  return raw && raw.trim().length > 0 ? raw.trim() : undefined;
};

export const getServerEnv = (): SozoServerEnv => {
  const missing = required.filter((key) => !value(key));
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  }

  return {
    azureResourceGroup: value("SOZO_AZURE_RESOURCE_GROUP")!,
    azureLocation: value("SOZO_AZURE_LOCATION")!,
    storageAccount: value("SOZO_STORAGE_ACCOUNT")!,
    storageAccountKey:
      value("SOZO_STORAGE_ACCOUNT_KEY") ?? value("AZURE_STORAGE_KEY"),
    serviceBusNamespace: value("SOZO_SERVICEBUS_NAMESPACE")!,
    serviceBusPolicyName:
      value("SOZO_SERVICEBUS_POLICY_NAME") ?? value("SOZO_SERVICEBUS_SAS_KEY_NAME"),
    serviceBusPolicyKey:
      value("SOZO_SERVICEBUS_POLICY_KEY") ?? value("SOZO_SERVICEBUS_SAS_KEY"),
    sqlHost: value("SOZO_SQL_HOST")!,
    sqlDb: value("SOZO_SQL_DB")!,
    sqlUser: value("SOZO_SQL_USER"),
    sqlPassword: value("SOZO_SQL_PASSWORD"),
    searchServiceName: value("SOZO_SEARCH_SERVICE_NAME")!,
    searchAdminKey: value("SOZO_SEARCH_ADMIN_KEY"),
    searchIndexName: value("SOZO_SEARCH_INDEX_NAME"),
    openAiEndpoint:
      value("SOZO_OPENAI_ENDPOINT") ?? value("AZURE_OPENAI_ENDPOINT"),
    openAiKey: value("SOZO_OPENAI_API_KEY") ?? value("AZURE_OPENAI_API_KEY"),
    openAiDeployment:
      value("SOZO_OPENAI_CHAT_DEPLOYMENT") ??
      value("AZURE_OPENAI_CHAT_DEPLOYMENT"),
    openAiApiVersion:
      value("SOZO_OPENAI_API_VERSION") ??
      value("AZURE_OPENAI_API_VERSION") ??
      "2024-10-21",
  };
};

export const looksConfigured = (v?: string) =>
  Boolean(v && !v.includes("az keyvault secret set"));
