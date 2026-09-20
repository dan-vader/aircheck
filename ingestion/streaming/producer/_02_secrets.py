from typing import Any

from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient


async def get_secrets(cfg: dict[str, Any]) -> dict[str, str]:
    vault_name = cfg["key_vault_name"]
    vault_url = f"https://{vault_name}.vault.azure.net/"

    secret_keys = cfg["secrets"]

    async with DefaultAzureCredential() as credential, \
            SecretClient(vault_url=vault_url, credential=credential) as client:
        contact_email = (await client.get_secret(secret_keys["contact_email"])).value
        eventhub_conn_str = (await client.get_secret(secret_keys["eventhub_conn_str"])).value

    return {"contact_email": contact_email, "eventhub_conn_str": eventhub_conn_str}