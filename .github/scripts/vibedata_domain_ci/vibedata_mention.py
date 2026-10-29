"""Core of the @vibedata mention runner.

A reviewer's `@vibedata <request>` comment on a pull request starts a short "mention"
Intent in Vibedata, as that reviewer, and hands it the handling-pr-mention skill prompt.
The skill does the real work; this module only decides whether to start it and makes
the calls that do.

Public surface:
    build_skill_prompt(...) -> str   the prompt sent to the mention Intent
    run_mention(event, ...) -> int   the whole run; returns the process exit code

Studio and GitHub are reached through the StudioPort and GitHubPort protocols, which the
runner shell implements; every decision lives here.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Protocol

ALLOWED_PERMISSIONS: frozenset[str] = frozenset({"write", "maintain", "admin"})
RETRYABLE_OPEN_CODES: frozenset[str] = frozenset({"intent_open_coordination_busy", "agent_server_startup_crashed"})
# GitHub lets a run be re-run for 30 days, but Studio forgets an idempotency key after a day,
# so an old comment re-run would create a second Intent for the same request.
MAX_COMMENT_AGE: timedelta = timedelta(hours=1)

ENVIRONMENT = "vibedata-mention"
PROMPT_FIRST_LINE = "Use the handling-pr-mention skill with this input:"
CALL_TIMEOUT = 20.0
# Open starts a fresh agent container, which alone takes several seconds before any load.
OPEN_TIMEOUT = 90.0
OPEN_RETRIES = 2
OPEN_RETRY_PAUSE = 5

REPLIES: dict[str, str] = {
    "unrestricted_environment": (
        "The vibedata-mention environment must be limited to the default branch before @vibedata can run. "
        "A repository admin sets this in Settings → Environments → vibedata-mention → Deployment branches "
        "and tags: choose Selected branches and tags, with one rule for the default branch only."
    ),
    "missing_pat": (
        "Add your Vibedata personal access token as the secret VIBEDATA_PAT_{user_id} in this repository's "
        "vibedata-mention environment. A repository admin creates that environment once; see "
        '"Use your key for @vibedata mentions in GitHub" in the Vibedata user guide.'
    ),
    "pr_unreadable": "Vibedata could not read this pull request. Comment again to retry.",
    "unauthorized": (
        "Your Vibedata personal access token is invalid or expired. Create a new one in Vibedata "
        "and update your VIBEDATA_PAT_{user_id} secret."
    ),
    "forbidden": "You cannot create Intents in this Vibedata Domain.",
    "could_not_start": "Vibedata could not start the request. Comment again to retry.",
}


@dataclass(frozen=True)
class MentionEvent:
    repository: str
    pull_request: int
    comment_id: int
    comment_body: str
    comment_created_at: datetime  # timezone-aware UTC
    commenter_login: str
    commenter_id: int
    default_branch: str


@dataclass(frozen=True)
class StudioResponse:
    status: int
    body: dict
    etag: str | None


class StudioError(Exception):
    """A non-2xx Studio response, or `status=None` when Studio was unreachable or timed out."""

    def __init__(self, status: int | None, code: str | None = None, detail: str | None = None):
        super().__init__(f"Studio {status} {code}: {detail}")
        self.status = status
        self.code = code
        self.detail = detail


class StudioPort(Protocol):
    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
        timeout: float,
    ) -> StudioResponse: ...


class GitHubPort(Protocol):
    def permission(self, repository: str, login: str) -> str: ...

    def environment_deployment_branches(self, repository: str, name: str) -> list[tuple[str, str]] | None:
        """The environment's custom deployment rules as (type, name); None when it has no custom rules."""
        ...

    def head_ref(self, repository: str, pull_request: int) -> str: ...

    def comment(self, repository: str, pull_request: int, body: str) -> None: ...

    def react_eyes(self, repository: str, comment_id: int) -> None: ...


def build_skill_prompt(
    *, repository: str, pull_request: int, head_branch: str, comment: str, studio_url: str, domain_id: str
) -> str:
    # json.dumps keeps the comment one escaped string, so text shaped like another key
    # can never add or replace a field the skill reads.
    payload = {
        "repository": repository,
        "pull_request": pull_request,
        "head_branch": head_branch,
        "comment": comment,
        "studio_url": studio_url.rstrip("/"),
        "domain_id": domain_id,
    }
    return PROMPT_FIRST_LINE + "\n" + json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _studio_failure(error: StudioError, user_id: int) -> tuple[str, str]:
    """(outcome, reply) for a Studio call that failed."""
    if error.status == 401:
        return "unauthorized", REPLIES["unauthorized"].format(user_id=user_id)
    if error.status == 403:
        return "forbidden", REPLIES["forbidden"]
    reply = REPLIES["could_not_start"]
    if error.status is not None:
        code_suffix = f" {error.code}" if error.code else ""
        detail = f": {error.detail}" if error.detail else ""
        reply += f" (Vibedata {error.status}{code_suffix}{detail})"
    return "could_not_start", reply


def _open(studio: StudioPort, path: str, headers: dict[str, str], sleep: Callable[[float], None]) -> None:
    for attempt in range(OPEN_RETRIES + 1):
        try:
            studio.request("POST", path, headers=headers, timeout=OPEN_TIMEOUT)
            return
        except StudioError as error:
            retryable = error.status == 503 and error.code in RETRYABLE_OPEN_CODES
            if not retryable or attempt == OPEN_RETRIES:
                raise
            sleep(OPEN_RETRY_PAUSE)


def _start_intent(
    event: MentionEvent, prompt: str, *, pat: str, domain_id: str, studio: StudioPort, sleep: Callable[[float], None],
    on_created: Callable[[str], None],
) -> str:
    """Create, activate, open and message the mention Intent. Returns `sent` or `rerun`."""
    auth = {"Authorization": f"Bearer {pat}"}
    created = studio.request(
        "POST",
        "/api/v1/intents",
        body={
            "domainId": domain_id,
            "title": f"PR #{event.pull_request} @vibedata mention",
            "agent": "build",
            "requiresIntentResources": False,
            "creationSource": "gha",
        },
        headers={**auth, "Idempotency-Key": f"gh-mention-{event.comment_id}"},
        timeout=CALL_TIMEOUT,
    )
    intent_id = (created.body.get("data") or {}).get("id")
    if not isinstance(intent_id, str) or not intent_id:
        raise StudioError(created.status, None, "create returned no Intent id")
    on_created(intent_id)
    intent_path = f"/api/v1/intents/{intent_id}"

    # A replayed create looks exactly like the first one, so the Intent's state is what tells
    # a re-run apart: only this runner's activate makes it ACTIVE.
    current = studio.request("GET", intent_path, headers=auth, timeout=CALL_TIMEOUT)
    if (current.body.get("data") or {}).get("lifecycleState") == "ACTIVE":
        return "rerun"

    activated = studio.request("POST", f"{intent_path}/activate", headers=auth, timeout=CALL_TIMEOUT)
    if not activated.etag:
        raise StudioError(activated.status, None, "activation returned no ETag")
    _open(studio, f"{intent_path}/open", {**auth, "If-Match": activated.etag}, sleep)
    studio.request(
        "POST", f"{intent_path}/conversation/send-message", body={"message": prompt}, headers=auth, timeout=CALL_TIMEOUT
    )
    return "sent"


def run_mention(
    event: MentionEvent,
    *,
    pat: str,
    studio_url: str,
    domain_id: str,
    studio: StudioPort,
    github: GitHubPort,
    now: Callable[[], datetime],
    log: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    intent: list[str] = []

    def finish(outcome: str, code: int, reply: str | None = None) -> int:
        if reply is not None:
            try:
                github.comment(event.repository, event.pull_request, reply.replace(pat, "***") if pat else reply)
            except Exception:
                # The outcome line below is the run's only other record, so a failed
                # reply must not prevent it.
                code = 1
        log(f"::notice::vibedata-mention outcome={outcome} intent={intent[0] if intent else 'none'}")
        return code

    # When GitHub cannot answer, the commenter's access and the environment's branch
    # rule are unknown, so nothing is sent and no reply is attempted through the same API.
    try:
        permission = github.permission(event.repository, event.commenter_login)
    except Exception:
        return finish("github_unavailable", 1)
    if permission not in ALLOWED_PERMISSIONS:
        return finish("not_writer", 0)
    if now() - event.comment_created_at > MAX_COMMENT_AGE:
        return finish("stale_comment", 0)
    # GitHub creates a referenced environment that does not exist with no protection rules,
    # so the first mention could otherwise release PAT secrets to any branch's workflow.
    # Only a single rule naming the default branch keeps them there: "protected branches",
    # a wildcard, a second branch or a tag rule each lets a writer's push reach them.
    try:
        rules = github.environment_deployment_branches(event.repository, ENVIRONMENT)
    except Exception:
        return finish("github_unavailable", 1)
    if rules != [("branch", event.default_branch)]:
        return finish("unrestricted_environment", 1, REPLIES["unrestricted_environment"])
    if not pat:
        return finish("missing_pat", 1, REPLIES["missing_pat"].format(user_id=event.commenter_id))
    try:
        head_branch = github.head_ref(event.repository, event.pull_request)
    except Exception:
        return finish("pr_unreadable", 1, REPLIES["pr_unreadable"])

    prompt = build_skill_prompt(
        repository=event.repository,
        pull_request=event.pull_request,
        head_branch=head_branch,
        comment=event.comment_body,
        studio_url=studio_url,
        domain_id=domain_id,
    )
    try:
        outcome = _start_intent(
            event, prompt, pat=pat, domain_id=domain_id, studio=studio, sleep=sleep, on_created=intent.append
        )
    except StudioError as error:
        outcome, reply = _studio_failure(error, event.commenter_id)
        return finish(outcome, 1, reply)

    try:
        github.react_eyes(event.repository, event.comment_id)
    except Exception:
        # The request already reached the mention Intent; a missing reaction must not
        # report the run as failed.
        pass
    return finish(outcome, 0)
