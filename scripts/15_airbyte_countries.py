"""Configura REST Countries → MinIO pela API local; não grava credenciais."""
import argparse
import json
import os
import re
import subprocess
import time

import requests
from course_config import ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configurar", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--aguardar", action="store_true", help="Acompanha o job mais recente sem criar outra carga")
    parser.add_argument("--corrigir-destino", action="store_true", help="Aplica o S3 Path Format padrão ao destino desta conexão")
    args = parser.parse_args()
    credentials = subprocess.run(["abctl", "local", "credentials"], capture_output=True, text=True, check=True)
    text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", credentials.stdout + credentials.stderr)
    creds = {k.lower(): v.strip() for k, v in re.findall(
        r"^\s*(Client-Id|Client-Secret):\s*(.+)$", text, re.M)}
    api_key = os.getenv("RESTCOUNTRIES_API_KEY", "")
    sensitive = [api_key, *creds.values()]
    base = "http://localhost:8000/api/public/v1/"
    session = requests.Session()

    def call(method, path, payload=None):
        response = session.request(method, base + path, json=payload, timeout=600)
        if response.status_code == 401 and path != "applications/token":
            refreshed = requests.post(base + "applications/token", json={
                "client_id": creds["client-id"], "client_secret": creds["client-secret"]}, timeout=30)
            refreshed.raise_for_status()
            new_token = refreshed.json()["access_token"]
            sensitive.append(new_token)
            session.headers["Authorization"] = "Bearer " + new_token
            response = session.request(method, base + path, json=payload, timeout=600)
        if (method == "POST" and path == "jobs" and response.status_code == 409
                and "A sync is already running" in response.text):
            return {"jobId": None, "status": "already_running", "jobType": "sync"}
        if not response.ok:
            message = response.text
            for value in sensitive:
                if value:
                    message = message.replace(value, "[REDACTED]")
            raise RuntimeError(f"Airbyte {method} {path}: HTTP {response.status_code}: {message[:1500]}")
        return response.json()

    token = call("POST", "applications/token", {
        "client_id": creds["client-id"], "client_secret": creds["client-secret"]})["access_token"]
    sensitive.append(token)
    session.headers["Authorization"] = "Bearer " + token
    workspace = call("GET", "workspaces")["data"][0]["workspaceId"]
    connections = call("GET", "connections")["data"]
    connection = next((c for c in connections if c["name"] == "countries-to-bronze"), None)
    if args.configurar and connection is None:
        if not api_key:
            raise RuntimeError("Defina RESTCOUNTRIES_API_KEY no .env")
        definitions_path = f"workspaces/{workspace}/definitions/declarative_sources"
        definitions = call("GET", definitions_path)
        definitions = definitions.get("data", []) if isinstance(definitions, dict) else definitions
        definition = next((d for d in definitions if d["name"] == "rest-countries-api-v5"), None)
        if definition is None:
            definition = call("POST", definitions_path, {
                "name": "rest-countries-api-v5",
                "manifest": json.loads((ROOT / "airbyte/restcountries_manifest.json").read_text()),
            })
        sources = call("GET", "sources")["data"]
        source = next((s for s in sources if s["name"] == "rest-countries-api"), None)
        if source is None:
            source = call("POST", "sources", {
                "name": "rest-countries-api", "workspaceId": workspace,
                "definitionId": definition["id"], "configuration": {"api_key": api_key},
            })
        destinations = call("GET", "destinations")["data"]
        destination = next(d for d in destinations if d["name"] == "minio-bronze-local")
        connection = call("POST", "connections", {
            "name": "countries-to-bronze", "sourceId": source["sourceId"],
            "destinationId": destination["destinationId"],
            "schedule": {"scheduleType": "manual"},
            "namespaceDefinition": "custom_format", "namespaceFormat": "rest-countries-api",
        })
    if connection is None:
        raise RuntimeError("Conexão ainda não criada; execute com --configurar")
    if args.corrigir_destino:
        call("PATCH", f"destinations/{connection['destinationId']}", {"configuration": {
            "s3_path_format": "${NAMESPACE}/${STREAM_NAME}/${YEAR}_${MONTH}_${DAY}_${EPOCH}_"
        }})
        print("S3 Path Format corrigido para o padrão documentado", flush=True)
    print(json.dumps({k: connection.get(k) for k in ["connectionId", "name", "status"]}, indent=2))
    if args.sync:
        job = call("POST", "jobs", {"connectionId": connection["connectionId"], "jobType": "sync"})
        print(json.dumps({k: job.get(k) for k in ["jobId", "status", "jobType"]}, indent=2))
    output = ROOT / "data/validation"
    output.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 1800
    while True:
        jobs = call("GET", f"jobs?connectionId={connection['connectionId']}&limit=5")["data"]
        jobs.sort(key=lambda j: int(j["jobId"]), reverse=True)
        summary = [{k: j.get(k) for k in ["jobId", "status", "rowsSynced", "bytesSynced"]} for j in jobs]
        print(json.dumps(summary, indent=2), flush=True)
        (output / "airbyte_jobs.json").write_text(json.dumps(summary, indent=2))
        if not args.aguardar or not jobs:
            break
        if jobs[0]["status"] == "succeeded":
            break
        if jobs[0]["status"] in ["failed", "cancelled"]:
            raise RuntimeError("O job Airbyte terminou sem sucesso; consulte os logs na interface")
        if time.monotonic() >= deadline:
            raise TimeoutError("A sincronização não terminou em 30 minutos")
        time.sleep(20)


if __name__ == "__main__":
    main()
