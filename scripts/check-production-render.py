#!/usr/bin/env python3
"""Checks the chart rendered with values-production.yaml (M18) against the
production platform's conventions: images from the platform's registry at
one commit, every container's credentials from platform-credentials and
proxies, each component's own Postgres role (the signer none, the migrate
hook the owner's), no topic hook, KEDA maxima at most 3 or the partitions.
Usage: check-production-render.py RENDER.yaml"""
import sys

import yaml

REGISTRY = "registry.digitalocean.com/securebrand/"
ROLES = {"api": "API", "scheduler": "SCHEDULER", "persister": "PERSISTER", "signer": None, "canary": "CRAWLER",
         "media-downloader": "CRAWLER", "crawler-account": "CRAWLER", "crawler-post": "CRAWLER",
         "crawler-thread": "CRAWLER", "crawler-retry": "CRAWLER"}


def env_of(container):
    out = {}
    for e in container.get("env", []):
        ref = (e.get("valueFrom") or {}).get("secretKeyRef")
        out[e["name"]] = (ref["name"], ref["key"]) if ref else e.get("value")
    return out


def main(path):
    docs = [d for d in yaml.safe_load_all(open(path)) if d]
    errs = []
    seen = set()
    for d in docs:
        kind, name = d["kind"], d["metadata"]["name"]
        if kind in ("Deployment", "Job"):
            spec = d["spec"]["template"]["spec"]
            for c in spec["containers"]:
                if not c["image"].startswith(REGISTRY):
                    errs.append(f"{name}: image {c['image']} is not the platform's")
                env = env_of(c)
                if env.get("TKS_KAFKA__SASL__PASSWORD") != ("platform-credentials", "KAFKA_PASSWORD"):
                    errs.append(f"{name}: Kafka password not from platform-credentials")
                if env.get("TKS_REDIS__ADDR") != "$(VALKEY_HOST):$(VALKEY_PORT)":
                    errs.append(f"{name}: Valkey address not from platform-credentials")
                user = env.get("TKS_POSTGRES__USER")
                if kind == "Job":
                    if user != ("platform-credentials", "POSTGRES_USER") or "TKS_POSTGRES__ROLES__READER" not in env:
                        errs.append(f"{name}: the migrate hook must log in as the owner and grant the roles")
                    if "kafka" in c.get("args", []):
                        errs.append(f"{name}: the platform owns the topics")
                    continue
                comp = d["metadata"]["labels"]["app.kubernetes.io/component"]
                seen.add(comp)
                role = ROLES.get(comp, "?")
                want = None if role is None else ("platform-credentials", f"POSTGRES_{role}_USER")
                if user != want:
                    errs.append(f"{name}: Postgres user {user}, want {want}")
            vols = {v["name"]: v for v in spec.get("volumes", [])}
            if vols.get("secret-files", {}).get("secret", {}).get("secretName") != "proxies":
                errs.append(f"{name}: proxies not mounted from the Secret proxies")
        if kind == "ScaledObject":
            n = d["spec"]["maxReplicaCount"]
            if n > 3:
                errs.append(f"{name}: maxReplicaCount {n} over its topics' partitions")
            for t in d["spec"]["triggers"]:
                m = t["metadata"]
                if m.get("sasl") != "scram_sha256" or m.get("tls") != "enable" or "bootstrapServersFromEnv" not in m:
                    errs.append(f"{name}: KEDA trigger not on the platform's SCRAM over TLS brokers")
    missing = set(ROLES) - seen
    if missing:
        errs.append(f"components not rendered: {sorted(missing)}")
    for e in errs:
        print("production render:", e)
    if errs:
        sys.exit(1)
    print(f"production render: {len(docs)} objects follow the platform's conventions")


if __name__ == "__main__":
    main(sys.argv[1])
