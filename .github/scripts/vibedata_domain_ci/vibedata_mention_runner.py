"""Thin shell for the Vibedata mention workflow.

gather → call → dispatch:
  1. Mask the repository's Vibedata token before any other output.
  2. Read the issue_comment event from GITHUB_EVENT_PATH; the comment body is read here,
     never interpolated into a shell command.
  3. Call vibedata_mention.run_mention with a urllib Studio client and a `gh` GitHub client.
  4. Exit with its code.

All decisions live in vibedata_mention.run_mention; this module owns only I/O.

Usage:
    python3 .github/scripts/vibedata_domain_ci/vibedata_mention_runner.py

Environment:
    GITHUB_EVENT_PATH  the issue_comment event payload
    GITHUB_TOKEN       the job token, used by `gh`
    STUDIO             the Vibedata URL
    DOMAIN_ID          the Vibedata Domain id
    PAT                the VIBEDATA_GHA_TOKEN secret, a Vibedata personal access token (empty when not set up)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

from vibedata_domain_ci.vibedata_mention import MentionEvent, StudioError, StudioResponse, run_mention


def event_from_payload(payload: dict) -> MentionEvent:
    comment = payload["comment"]
    created_at = datetime.fromisoformat(comment["created_at"].replace("Z", "+00:00")).astimezone(timezone.utc)
    return MentionEvent(
        repository=payload["repository"]["full_name"],
        pull_request=int(payload["issue"]["number"]),
        comment_id=int(comment["id"]),
        comment_body=comment["body"],
        comment_created_at=created_at,
        commenter_login=comment["user"]["login"],
        default_branch=payload["repository"]["default_branch"],
    )


def _json_or_empty(raw: bytes) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class UrllibStudio:
    def __init__(self, base_url: str, pat: str):
        self._base_url = base_url.rstrip("/")
        self._pat = pat

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
        timeout: float,
    ) -> StudioResponse:
        data = json.dumps(body).encode() if body is not None else None
        try:
            # Built inside the try: Request rejects an empty or malformed URL with ValueError.
            req = urllib.request.Request(self._base_url + path, data=data, method=method)
            req.add_header("Accept", "application/json")
            if data is not None:
                req.add_header("Content-Type", "application/json")
            for name, value in (headers or {}).items():
                req.add_header(name, value)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return StudioResponse(resp.status, _json_or_empty(resp.read()), resp.headers.get("ETag"))
        except urllib.error.HTTPError as e:
            problem = _json_or_empty(e.read())
            detail = problem.get("detail")
            if detail and self._pat:
                detail = detail.replace(self._pat, "***")
            raise StudioError(e.code, problem.get("code"), detail) from None
        # ValueError: an unset or malformed Vibedata URL variable is unreachable, not a crash.
        except (OSError, ValueError):
            raise StudioError(None, None, None) from None


class GhCliGitHub:
    def _api(self, *args: str) -> str:
        return subprocess.run(["gh", "api", *args], capture_output=True, text=True, check=True).stdout

    def permission(self, repository: str, login: str) -> str:
        return self._api(f"repos/{repository}/collaborators/{login}/permission", "--jq", ".permission").strip()

    def environment_deployment_branches(self, repository: str, name: str) -> list[tuple[str, str]] | None:
        try:
            raw = self._api(f"repos/{repository}/environments/{name}")
        except subprocess.CalledProcessError as e:
            if "HTTP 404" in (e.stderr or ""):
                return None
            raise
        policy = json.loads(raw).get("deployment_branch_policy") or {}
        if not policy.get("custom_branch_policies"):
            return None
        rules = json.loads(self._api(f"repos/{repository}/environments/{name}/deployment-branch-policies"))
        return [(rule.get("type", "branch"), rule["name"]) for rule in rules.get("branch_policies", [])]

    def head_ref(self, repository: str, pull_request: int) -> str:
        return self._api(f"repos/{repository}/pulls/{pull_request}", "--jq", ".head.ref").strip()

    def comment(self, repository: str, pull_request: int, body: str) -> None:
        subprocess.run(
            ["gh", "pr", "comment", str(pull_request), "--repo", repository, "--body-file", "-"],
            input=body, capture_output=True, text=True, check=True,
        )

    def react_eyes(self, repository: str, comment_id: int) -> None:
        self._api("--method", "POST", f"repos/{repository}/issues/comments/{comment_id}/reactions", "-f", "content=eyes")


def main() -> int:
    pat = os.environ.get("PAT", "")
    if pat:
        print(f"::add-mask::{pat}", flush=True)
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as f:
        event = event_from_payload(json.load(f))
    studio_url = os.environ.get("STUDIO", "")
    return run_mention(
        event,
        pat=pat,
        studio_url=studio_url,
        domain_id=os.environ.get("DOMAIN_ID", ""),
        studio=UrllibStudio(studio_url, pat),
        github=GhCliGitHub(),
        now=lambda: datetime.now(timezone.utc),
        log=print,
    )


if __name__ == "__main__":
    sys.exit(main())
