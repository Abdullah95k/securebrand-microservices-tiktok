#!/usr/bin/env python3
"""Run the Helm chart's workloads in Docker, as the chart defines them.

Without a Kubernetes cluster, this checks what a cluster would run: it
renders the chart (deploy/helm/tks/examples/fakett.yaml by default) and
starts every hook Job and Deployment container with the rendered command,
environment, ConfigMap and Secret mounts, user, read-only root filesystem,
dropped capabilities and memory limit, on the network of `make infra-up`
(Postgres, Redpanda, Valkey), next to fakett and an in-memory S3. fakett
refuses unsigned comment and reply calls when --browser-image is given,
so tks-signer's pages (Chromium in its hardened container, which the
crawlers and the canary reach by its network alias, as by its Service)
must sign them; without it the signer is off and fakett serves them
unsigned. Then it imports fakett's accounts through the API
(`POST /v1/accounts/import`) and waits for their backfills: every job must
complete, with posts, comments, replies and media stored, and no component
may log an error.

It does not exercise what only Kubernetes does: Services, probes, KEDA,
PodDisruptionBudgets. `kubeconform` checks those objects' schemas.

    python3 scripts/chart-smoke.py --image tks:dev --browser-image tks:dev-browser [--values FILE] [--keep]

It uses its own database (tks_smoke), Redis DB 15 and topic prefix
(chartsmoke.), so it can run next to the tests.
"""
import argparse
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request

import yaml

PREFIX = "tks-smoke-"
TOPIC_PREFIX = "chartsmoke."
DB = "tks_smoke"
# fakett's synthetic accounts (internal/fakett).
ACCOUNTS = ["account_0", "account_1", "account_2"]
INFRA = "tks-{}-1"  # make infra-up's containers (Compose project tks)


def sh(*args, check=True, capture=True, timeout=600):
    r = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise SystemExit(f"command failed ({r.returncode}): {' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return r


def psql(sql, db=DB):
    return sh("docker", "exec", INFRA.format("postgres"), "psql", "-U", "tks", "-d", db, "-tAc", sql).stdout.strip()


def render(args):
    repo, _, tag = args.image.rpartition(":")
    if not repo or "/" in tag:
        raise SystemExit(f"--image {args.image}: want repository:tag")
    sets = ["--set", f"image.repository={repo}", "--set", f"image.tag={tag}"]
    if args.browser_image:
        brepo, _, btag = args.browser_image.rpartition(":")
        if brepo != repo:
            raise SystemExit(f"--browser-image {args.browser_image}: want the repository of --image ({repo})")
        sets += ["--set", f"image.browserTag={btag}"]
    else:
        # No Chromium: nothing signs, so comments and replies are not read.
        sets += ["--set", "config.tt.signer.mode=none", "--set", "components.canary.enabled=false",
                 "--set", "components.signer.enabled=false"]
    out = sh(args.helm, "template", "tks", "deploy/helm/tks", "-f", args.values, *sets,
             "--set", "image.pullPolicy=Never",
             "--set", f"config.kafka.topic_prefix={TOPIC_PREFIX}",
             "--set", "config.redis.db=15").stdout
    return [d for d in yaml.safe_load_all(out) if d]


def docker_args(name, alias, pod, container, configmaps, secret_env, network):
    """docker run arguments for one pod container, from its rendered spec;
    configmaps maps a ConfigMap's name to the directory holding its files."""
    psc = pod.get("securityContext") or {}
    csc = container.get("securityContext") or {}
    a = ["docker", "run", "-d", "--name", PREFIX + name, "--network", network, "--network-alias", alias,
         "--env-file", secret_env]
    if psc.get("runAsUser"):
        a += ["--user", f"{psc['runAsUser']}:{psc.get('fsGroup', psc['runAsUser'])}"]
    if csc.get("readOnlyRootFilesystem"):
        a.append("--read-only")
    for cap in (csc.get("capabilities") or {}).get("drop") or []:
        a += ["--cap-drop", cap]
    if csc.get("allowPrivilegeEscalation") is False:
        a += ["--security-opt", "no-new-privileges"]
    mem = ((container.get("resources") or {}).get("limits") or {}).get("memory")
    if mem:
        a += ["--memory", mem.replace("Mi", "m").replace("Gi", "g")]
    for e in container.get("env") or []:
        # valueFrom (POD_IP) has no Docker equivalent: tks-signer then
        # answers without its own address, and callers use its Service's.
        if "value" in e:
            a += ["-e", f"{e['name']}={e['value']}"]
    volumes = {v["name"]: v for v in pod.get("volumes") or []}
    for m in container.get("volumeMounts") or []:
        v = volumes[m["name"]]
        if "configMap" in v:
            a += ["-v", f"{configmaps[v['configMap']['name']]}:{m['mountPath']}:ro"]
        elif "emptyDir" in v:
            # An emptyDir is not mounted noexec in Kubernetes; Docker's
            # tmpfs is by default, and Chromium (--disable-dev-shm-usage)
            # keeps its shared memory there.
            a += ["--tmpfs", f"{m['mountPath']}:exec,uid={psc.get('runAsUser', 0)},gid={psc.get('fsGroup', 0)}"]
        else:
            raise SystemExit(f"{name}: volume {m['name']} has no Docker equivalent here: {v}")
    for p in container.get("ports") or []:
        a += ["-p", f"127.0.0.1::{p['containerPort']}"]
    cmd = list(container.get("command") or []) + list(container.get("args") or [])
    a += ["--entrypoint", cmd[0], container["image"]] + cmd[1:]
    return a


def host_port(name, port):
    out = sh("docker", "port", PREFIX + name, str(port)).stdout.strip().splitlines()[0]
    return int(out.rsplit(":", 1)[1])


def http(url, key=None, body=None, method=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, method=method)
    if key:
        req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except OSError as e:
        return 0, str(e)


def cleanup():
    names = sh("docker", "ps", "-aq", "--filter", f"name={PREFIX}").stdout.split()
    if names:
        sh("docker", "rm", "-f", *names, check=False)
    sh("docker", "exec", INFRA.format("postgres"), "psql", "-U", "tks", "-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)", check=False)
    sh("docker", "exec", INFRA.format("redpanda"), "rpk", "topic", "delete", "-r", "^" + TOPIC_PREFIX.replace(".", r"\.") + ".*", check=False)
    sh("docker", "exec", INFRA.format("valkey"), "valkey-cli", "-n", "15", "flushdb", check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", default="tks:dev")
    ap.add_argument("--browser-image", default="", help="the --target browser image (same repository): the signer's pages sign comments and replies")
    ap.add_argument("--values", default="deploy/helm/tks/examples/fakett.yaml")
    ap.add_argument("--network", default="tks_default")
    ap.add_argument("--helm", default="helm")
    ap.add_argument("--timeout", type=int, default=420, help="seconds for the backfills")
    ap.add_argument("--keep", action="store_true", help="leave the containers running")
    args = ap.parse_args()

    # make infra-up returns once Postgres answers; Redpanda may still be
    # starting. A fresh broker also answers its first producer-ID requests
    # with NOT_COORDINATOR for a while: produce once (idempotently, as the
    # components do) before starting them.
    sh("docker", "exec", INFRA.format("redpanda"), "rpk", "cluster", "health", "--exit-when-healthy", timeout=180)
    cleanup()
    deadline = time.time() + 120
    while sh("docker", "exec", INFRA.format("redpanda"), "sh", "-c",
             f"rpk topic create {TOPIC_PREFIX}warmup >/dev/null 2>&1; echo warmup | rpk topic produce {TOPIC_PREFIX}warmup", check=False).returncode != 0:
        if time.time() > deadline:
            raise SystemExit("Redpanda does not accept idempotent produce requests")
        time.sleep(2)
    docs = render(args)
    configmaps = {d["metadata"]["name"]: d.get("data") or {} for d in docs if d["kind"] == "ConfigMap"}
    hooks = [d for d in docs if d["kind"] == "Job"]
    deployments = [d for d in docs if d["kind"] == "Deployment"]
    print(f"rendered: {len(deployments)} deployments, {len(hooks)} hook jobs, configmaps {sorted(configmaps)}")

    psql(f"CREATE DATABASE {DB}", db="postgres")
    work = tempfile.mkdtemp(prefix="chart-smoke-")
    os.chmod(work, 0o755)
    # Each ConfigMap as a directory, written once: rewriting one while a
    # container reads it would hand that container an empty file.
    for cm, data in list(configmaps.items()):
        d = os.path.join(work, cm)
        os.makedirs(d)
        os.chmod(d, 0o755)
        for fname, body in data.items():
            with open(os.path.join(d, fname), "w") as f:
                f.write(body)
            os.chmod(os.path.join(d, fname), 0o644)
        configmaps[cm] = d
    # What the cluster's Secret tks-env (secretEnv.existingSecret) would hold.
    secret_env = os.path.join(work, "tks-env")
    with open(secret_env, "w") as f:
        f.write(f"TKS_POSTGRES__DSN=postgres://tks:tks@postgres:5432/{DB}?sslmode=disable\n"
                "TKS_S3__ACCESS_KEY=smoke\nTKS_S3__SECRET_KEY=smokesmoke\n"
                f"TKS_PRIVACY__AUTHOR_HMAC_KEY={secrets.token_hex(32)}\n")
    # The Services the values expect besides the infrastructure.
    sh("docker", "run", "-d", "--name", PREFIX + "s3", "--network", args.network, "--network-alias", "s3",
       "--entrypoint", "/usr/local/bin/devs3", args.image, "--addr", ":9000")
    sh("docker", "run", "-d", "--name", PREFIX + "fakett", "--network", args.network, "--network-alias", "fakett",
       "--entrypoint", "/usr/local/bin/fakett", args.image, "--addr", ":8090", "--accounts", str(len(ACCOUNTS)),
       "--posts", "8", "--max-comments", "25", "--max-replies", "4",
       # Without Chromium nothing signs: fakett then serves comments unsigned.
       *(["--require-signature"] if args.browser_image else []))

    failures = []
    try:
        # Hooks (pre-install), in order, each must exit 0.
        for job in sorted(hooks, key=lambda j: j["metadata"]["name"].endswith("-topics")):
            name = job["metadata"]["name"]
            pod = job["spec"]["template"]["spec"]
            a = docker_args(name, name, pod, pod["containers"][0], configmaps, secret_env, args.network)
            cid = sh(*a).stdout.strip()
            code = sh("docker", "wait", cid, timeout=180).stdout.strip()
            r = sh("docker", "logs", cid, check=False)
            logs = r.stdout + r.stderr
            print(f"hook {name}: exit {code}")
            if code != "0":
                raise SystemExit(f"hook {name} failed:\n{logs[-3000:]}")

        # Chromium in tks-signer's container, as the pod runs it: a render
        # that fails here says why, where the signer only counts errors.
        for d in deployments:
            pod = d["spec"]["template"]["spec"]
            c = pod["containers"][0]
            if not args.browser_image or not c["image"].endswith(":" + args.browser_image.rpartition(":")[2]):
                continue
            a = docker_args("chromium-check", "chromium-check", pod, c, configmaps, secret_env, args.network)
            a = a[:a.index("--entrypoint")]
            a = [x for x in a if x != "-d"]
            a[a.index("run") + 1:a.index("run") + 1] = ["--rm"]
            r = sh(*[x for x in a if not x.startswith("127.0.0.1::") and x != "-p"], "--entrypoint", "/usr/bin/chromium",
                   c["image"], "--headless=new", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
                   "--dump-dom", "data:text/html,<p>chromium-ok</p>", check=False, timeout=120)
            if "chromium-ok" not in r.stdout:
                raise SystemExit(f"Chromium does not render in {d['metadata']['name']}'s container "
                                 f"(exit {r.returncode}):\n{r.stdout[-2000:]}\n{r.stderr[-4000:]}")
            print(f"chromium renders in {d['metadata']['name']}'s container")
            break

        for d in deployments:
            name = d["metadata"]["name"]
            pod = d["spec"]["template"]["spec"]
            sh(*docker_args(name, name, pod, pod["containers"][0], configmaps, secret_env, args.network))
        # Readiness, as the readinessProbe would check it.
        deadline = time.time() + 120
        pending = {d["metadata"]["name"] for d in deployments}
        while pending and time.time() < deadline:
            for name in sorted(pending):
                status, _ = http(f"http://127.0.0.1:{host_port(name, 9090)}/readyz")
                if status == 200:
                    pending.discard(name)
            time.sleep(1)
        if pending:
            raise SystemExit(f"not ready after 120s: {sorted(pending)}")
        print(f"all {len(deployments)} deployments ready")

        # An API key, as docs/runbooks/tks-deploy.md creates it.
        out = sh("docker", "exec", PREFIX + "tks-api", "/usr/local/bin/tksctl", "apikey", "create", "--name", "smoke",
                 "--scopes", "accounts:write,jobs:write,data:read,pii:read").stdout
        key = json.loads(out[out.index("{"):])["api_key"]
        api = f"http://127.0.0.1:{host_port('tks-api', 8080)}"
        status, body = http(api + "/v1/accounts/import", key, {"lines": ACCOUNTS, "labels": ["smoke"]}, "POST")
        imported = json.loads(body) if status == 200 else {}
        print(f"accounts import: {status} {body[:300]}")
        if imported.get("added") != len(ACCOUNTS):
            failures.append(f"accounts import added {imported.get('added')}, want {len(ACCOUNTS)}")
        # Importing again changes nothing.
        status, body = http(api + "/v1/accounts/import", key, {"lines": ACCOUNTS}, "POST")
        again = json.loads(body) if status == 200 else {}
        if again.get("already_monitored") != len(ACCOUNTS) or again.get("added"):
            failures.append(f"second import: {status} {body[:300]}")

        # Each account's backfill (admitted at once: no budget is set).
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            done = int(psql("SELECT count(*) FROM tt.jobs WHERE kind = 'backfill' AND status IN ('completed','partial','failed','cancelled')") or 0)
            if done >= len(ACCOUNTS):
                break
            time.sleep(3)
        statuses = psql("SELECT string_agg(kind || ':' || status, ',' ORDER BY kind) FROM tt.jobs")
        posts = int(psql("SELECT count(*) FROM tt.posts"))
        comments = int(psql("SELECT count(*) FROM tt.comments WHERE parent_comment_id IS NULL") or 0)
        replies = int(psql("SELECT count(*) FROM tt.comments WHERE parent_comment_id IS NOT NULL") or 0)
        media = psql("SELECT coalesce(string_agg(status || '=' || n, ','), '') FROM (SELECT status, count(*) n FROM tt.media GROUP BY status) m")
        print(f"jobs {statuses}; posts {posts}; comments {comments}; replies {replies}; media {media}")
        backfills = [s for s in (statuses or "").split(",") if s.startswith("backfill:")]
        if len(backfills) != len(ACCOUNTS) or any(s != "backfill:completed" for s in backfills):
            failures.append(f"backfills: {statuses}")
        if posts == 0:
            failures.append("no posts stored")
        if comments == 0 or replies == 0:
            failures.append(f"comments {comments}, replies {replies}: want both")
        if "ready=" not in media:
            failures.append(f"no media downloaded: {media}")
        status, body = http(api + "/v1/accounts?label=smoke", key)
        if status != 200 or body.count('"username"') < len(ACCOUNTS):
            failures.append(f"GET /v1/accounts: {status} {body[:200]}")

        for d in deployments + hooks:
            name = d["metadata"]["name"]
            r = sh("docker", "logs", PREFIX + name, check=False)
            errors = [l for l in (r.stdout + r.stderr).splitlines() if '"level":"ERROR"' in l]
            if errors:
                failures.append(f"{name} logged {len(errors)} errors, e.g. {errors[0][:300]}")
        if failures:
            # The canary's last verdict per operation variant, with its
            # reason (a signer error says why there).
            print("== tt.operations (canary verdicts)")
            print(psql("SELECT string_agg(name || ' ' || doc_id || ': ' || coalesce(last_check, '-'), E'\\n' ORDER BY name) FROM tt.operations"))
            # What each component warned about, to diagnose without --keep
            # (CI): distinct messages, most frequent first.
            for d in deployments:
                name = d["metadata"]["name"]
                r = sh("docker", "logs", PREFIX + name, check=False)
                counts = {}
                for l in (r.stdout + r.stderr).splitlines():
                    if '"level":"WARN"' in l or '"level":"ERROR"' in l or not l.startswith("{"):
                        try:
                            e = json.loads(l)
                            key = " ".join(f"{k}={e[k]}" for k in ("level", "msg", "err", "reason", "error") if k in e)
                        except ValueError:
                            key = l
                        counts[key[:400]] = counts.get(key[:400], 0) + 1
                if counts:
                    print(f"== {name}: warnings and errors")
                    for k, n in sorted(counts.items(), key=lambda kv: -kv[1])[:12]:
                        print(f"  {n:5d}  {k}")
                print(f"== {name}: last lines")
                for l in (r.stdout + r.stderr).splitlines()[-8:]:
                    print("  " + l[:400])
    finally:
        if not args.keep:
            cleanup()

    if failures:
        print("FAIL")
        for f in failures:
            print(" -", f)
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
