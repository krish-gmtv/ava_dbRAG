import os
from dataclasses import dataclass
from typing import Optional

import requests
from dotenv import load_dotenv


load_dotenv()

AUTH_URL = "https://ava.andrew-chat.com/api/v1/user"


@dataclass(frozen=True)
class AvaAuthConfig:
    token: Optional[str] = None
    username: str = ""
    password: str = ""


def load_ava_auth_config() -> AvaAuthConfig:
    token = (os.environ.get("AVA_TOKEN") or "").strip() or None
    username = (os.environ.get("AVA_USERNAME") or "").strip()
    password = (os.environ.get("AVA_PASSWORD") or "").strip()
    return AvaAuthConfig(token=token, username=username, password=password)


def resolve_token() -> str:
    """
    Resolve Ava authorization token.
    Safety: prefer AVA_TOKEN; only call auth endpoint if token is missing
    and username/password exist.
    """
    cfg = load_ava_auth_config()
    if cfg.token:
        return cfg.token

    if not cfg.username or not cfg.password:
        raise RuntimeError(
            "Ava token not found. Set AVA_TOKEN (preferred) or AVA_USERNAME + AVA_PASSWORD."
        )

    resp = requests.post(
        AUTH_URL,
        headers={"Content-Type": "application/json"},
        json={"username": cfg.username, "password": cfg.password},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    token = (data.get("authorization") or "").strip()
    if not token:
        raise RuntimeError("Auth response missing 'authorization' token.")
    return token

