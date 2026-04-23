import { WarmHubClient } from "@warmhub/sdk-ts";

export function clientFromEnv(): WarmHubClient {
  const apiUrl = process.env.WARMHUB_API_URL;
  const token = process.env.WARMHUB_TOKEN;
  if (!apiUrl) throw new Error("WARMHUB_API_URL not set (sprite runtime should inject)");
  if (!token) throw new Error("WARMHUB_TOKEN not set (sprite runtime should inject)");
  return new WarmHubClient({ apiUrl, accessToken: () => token });
}

export function homeRepo(): string {
  const repo = process.env.WARMHUB_REPO;
  if (!repo) throw new Error("WARMHUB_REPO not set (sprite runtime should inject)");
  return repo;
}
