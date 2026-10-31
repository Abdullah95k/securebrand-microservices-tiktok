#!/usr/bin/env python3
"""Run the Helm chart's workloads in Docker, as the chart defines them.

Without a Kubernetes cluster, this checks what a cluster would run: it
renders the chart (deploy/helm/fbs/examples/fakefb.yaml by default) and
starts every hook Job and Deployment container with the rendered command,
environment, ConfigMap and Secret mounts, user, read-only root filesystem,
dropped capabilities and memory limit, on the network of `make infra-up`
(Postgres, Redpanda, Valkey), next to fakefb and an in-memory S3. Then it
registers fakefb's pages through the API (`fbctl pages import`) and waits
for their backfills: every job must complete, with comments and media
stored, and no component may log an error.

It does not exercise what only Kubernetes does: Services, probes, KEDA,
PodDisruptionBudgets. `kubeconform` checks those objects' schemas.

    python3 scripts/chart-smoke.py --image fbs:dev [--values FILE] [--keep]

It uses its own database (fbs_smoke), Redis DB 15 and topic prefix
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

PREFIX = "fbs-smoke-"
TOPIC_PREFIX = "chartsmoke."
DB = "fbs_smoke"
PAGES = ["brand0", "brand1", "brand2"]


def sh(*args, check=True, capture=True, timeout=600):
    r = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise SystemExit(f"command failed ({r.returncode}): {' '.join(args)}\n{r.stdout}\n{r.stderr}")
    return r


def psql(sql, db=DB):
    return sh("docker", "exec", "fbs-postgres-1", "psql", "-U", "fbs", "-d", db, "-tAc", sql).stdout.strip()


def render(args):
    repo, _, tag = args.image.rpartition(":")
    if not repo or "/" in tag:
        raise SystemExit(f"--image {args.image}: want repository:tag")
    out = sh(args.helm, "template", "fbs", "deploy/helm/fbs", "-f", args.values,
             "--set", f"image.repository={repo}",
             "--set", f"image.tag={tag}",
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
        a += ["-e", f"{e['name']}={e['value']}"]
    volumes = {v["name"]: v for v in pod.get("volumes") or []}
    for m in container.get("volumeMounts") or []:
        v = volumes[m["name"]]
        if "configMap" in v:
            a += ["-v", f"{configmaps[v['configMap']['name']]}:{m['mountPath']}:ro"]
        elif "emptyDir" in v:
            a += ["--tmpfs", f"{m['mountPath']}:uid={psc.get('runAsUser', 0)},gid={psc.get('fsGroup', 0)}"]
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
    sh("docker", "exec", "fbs-postgres-1", "psql", "-U", "fbs", "-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)", check=False)
    sh("docker", "exec", "fbs-redpanda-1", "rpk", "topic", "delete", "-r", "^" + TOPIC_PREFIX.replace(".", r"\.") + ".*", check=False)
    sh("docker", "exec", "fbs-valkey-1", "valkey-cli", "-n", "15", "flushdb", check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", default="fbs:dev")
    ap.add_argument("--values", default="deploy/helm/fbs/examples/fakefb.yaml")
    ap.add_argument("--network", default="fbs_default")
    ap.add_argument("--helm", default="helm")
    ap.add_argument("--timeout", type=int, default=300, help="seconds for the backfills")
    ap.add_argument("--keep", action="store_true", help="leave the containers running")
    args = ap.parse_args()

    # make infra-up returns once Postgres answers; Redpanda may still be
    # starting. A fresh broker also answers its first producer-ID requests
    # with NOT_COORDINATOR for a while: produce once (idempotently, as the
    # components do) before starting them.
    sh("docker", "exec", "fbs-redpanda-1", "rpk", "cluster", "health", "--exit-when-healthy", timeout=180)
    cleanup()
    deadline = time.time() + 120
    while sh("docker", "exec", "fbs-redpanda-1", "sh", "-c",
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
    # What the cluster's Secret fbs-env (secretEnv.existingSecret) would hold.
    secret_env = os.path.join(work, "fbs-env")
    with open(secret_env, "w") as f:
        f.write(f"IGS_POSTGRES__DSN=postgres://fbs:fbs@postgres:5432/{DB}?sslmode=disable\n"
                "IGS_S3__ACCESS_KEY=smoke\nIGS_S3__SECRET_KEY=smokesmoke\n"
                f"IGS_PRIVACY__AUTHOR_HMAC_KEY={secrets.token_hex(32)}\n")
    # The Services the values expect besides the infrastructure.
    sh("docker", "run", "-d", "--name", PREFIX + "s3", "--network", args.network, "--network-alias", "s3",
       "--entrypoint", "/usr/local/bin/devs3", args.image, "--addr", ":9000")
    sh("docker", "run", "-d", "--name", PREFIX + "fakefb", "--network", args.network, "--network-alias", "fakefb",
       "--entrypoint", "/usr/local/bin/fakefb", args.image, "--addr", ":8090", "--pages", str(len(PAGES)), "--posts", "10")

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

        # An API key, as the go-live runbook creates it.
        out = sh("docker", "exec", PREFIX + "fbs-api", "/usr/local/bin/fbctl", "apikey", "create", "--name", "smoke",
                 "--scopes", "pages:write,jobs:write,data:read").stdout
        key = json.loads(out[out.index("{"):])["api_key"]
        pages_file = os.path.join(work, "pages.csv")
        with open(pages_file, "w") as f:
            f.write("page\n" + "\n".join(PAGES) + "\n")
        os.chmod(pages_file, 0o644)
        out = sh("docker", "run", "--rm", "--network", args.network, "-v", f"{pages_file}:/pages.csv:ro",
                 "--entrypoint", "/usr/local/bin/fbctl", args.image, "pages", "import",
                 "--api", "http://fbs-api:8080", "--key", key, "/pages.csv").stdout
        imported = json.loads(out[out.index("{"):])
        print(f"pages import: {imported}")
        if imported.get("created") != len(PAGES):
            failures.append(f"pages import created {imported.get('created')}, want {len(PAGES)}")
        # Importing again changes nothing.
        out = sh("docker", "run", "--rm", "--network", args.network, "-v", f"{pages_file}:/pages.csv:ro",
                 "--entrypoint", "/usr/local/bin/fbctl", args.image, "pages", "import",
                 "--api", "http://fbs-api:8080", "--key", key, "/pages.csv").stdout
        again = json.loads(out[out.index("{"):])
        if again.get("existing") != len(PAGES) or again.get("created"):
            failures.append(f"second import: {again}")

        deadline = time.time() + args.timeout
        while time.time() < deadline:
            done = int(psql("SELECT count(*) FROM jobs WHERE status IN ('completed','partial','failed','cancelled')") or 0)
            if done >= len(PAGES):
                break
            time.sleep(2)
        statuses = psql("SELECT string_agg(kind || ':' || status, ',') FROM jobs")
        posts = int(psql("SELECT count(*) FROM posts"))
        comments = int(psql("SELECT count(*) FROM comments"))
        media = psql("SELECT coalesce(string_agg(status || '=' || n, ','), '') FROM (SELECT status, count(*) n FROM media GROUP BY status) m")
        print(f"jobs {statuses}; posts {posts}; comments {comments}; media {media}")
        if statuses is None or statuses.count(":completed") != len(PAGES):
            failures.append(f"jobs: {statuses}")
        if posts == 0 or comments == 0:
            failures.append("nothing stored")
        if "ready=" not in media and "deleted=" not in media:
            failures.append(f"no media downloaded: {media}")
        api_port = host_port("fbs-api", 8080)
        status, body = http(f"http://127.0.0.1:{api_port}/v1/pages", key)
        if status != 200:
            failures.append(f"GET /v1/pages: {status} {body[:200]}")

        for d in deployments + hooks:
            name = d["metadata"]["name"]
            r = sh("docker", "logs", PREFIX + name, check=False)
            errors = [l for l in (r.stdout + r.stderr).splitlines() if '"level":"ERROR"' in l]
            if errors:
                failures.append(f"{name} logged {len(errors)} errors, e.g. {errors[0][:300]}")
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
