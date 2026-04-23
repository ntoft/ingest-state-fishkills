import { WarmHubClient } from "@warmhub/sdk-ts";

const DEFAULT_HOME_REPO = "fish-kill-attribution/state-fishkills";

export function clientFromEnv(): WarmHubClient {
  const apiUrl = process.env.WARMHUB_API_URL ?? "https://api.warmhub.ai";
  const token = process.env.WH_TOKEN ?? process.env.WARMHUB_TOKEN;
  if (!token) throw new Error("WH_TOKEN not set (sprite runtime should inject)");
  return new WarmHubClient({ apiUrl, accessToken: () => token });
}

export function homeRepo(): string {
  return process.env.WARMHUB_HOME_REPO ?? process.env.WARMHUB_REPO ?? DEFAULT_HOME_REPO;
}

export function splitRepo(repo: string): { orgName: string; repoName: string } {
  const [orgName, repoName] = repo.split("/");
  if (!orgName || !repoName) {
    throw new Error(`Expected repo in "org/name" form, got ${JSON.stringify(repo)}`);
  }
  return { orgName, repoName };
}
