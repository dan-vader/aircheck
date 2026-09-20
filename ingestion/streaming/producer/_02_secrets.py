import asyncio
from typing import Any

from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient


async def get_secrets(cfg: dict[str, Any]) -> dict[str, str]:
    vault_name = cfg["key_vault_name"]
    vault_url = f"https://{vault_name}.vault.azure.net/"

    secret_keys = cfg["secrets"]

    async with DefaultAzureCredential() as credential, \
            SecretClient(vault_url=vault_url, credential=credential) as client:
        contact_email_secret, eventhub_conn_str_secret = await asyncio.gather(
            client.get_secret(secret_keys["contact_email"]),
            client.get_secret(secret_keys["eventhub_conn_str"]),
        )

    return {
        "contact_email": contact_email_secret.value,
        "eventhub_conn_str": eventhub_conn_str_secret.value,
    }