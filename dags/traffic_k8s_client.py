from pathlib import Path
from kubernetes import client

AUTH_DIR = Path("/opt/airflow/k8s-auth")

def get_k8s_client():
    """Create a Kubernetes API client using Airflow's dedicated ServiceAccount."""

    token = (AUTH_DIR / "airflow-token").read_text(encoding="utf-8").strip()

    config = client.Configuration()
    config.host = "https://host.docker.internal:60050"
    config.api_key = {"authorization": f"Bearer {token}"}
    config.ssl_ca_cert = str(AUTH_DIR / "ca.crt")
    config.verify_ssl = True
    config.assert_hostname = "kubernetes.default.svc"

    return client.ApiClient(config)
