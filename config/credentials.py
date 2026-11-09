"""Resolve credentials explicitly per account; never inherit process-wide keys."""
import os


def credential_pair(sources, fallback=None):
    for source in sources:
        api = source.get("api_key") or source.get("apiKey")
        secret = source.get("secret_key") or source.get("secKey")
        api_env, secret_env = source.get("api_key_env"), source.get("secret_key_env")
        key_type = str(source.get("key_type", "auto")).lower()
        if api_env or secret_env:
            if api or secret:
                raise ValueError("configure literal credentials OR named environment variables, not both")
            if not api_env or not secret_env:
                raise ValueError("api_key_env and secret_key_env must be configured together")
            api, secret = os.getenv(str(api_env)), os.getenv(str(secret_env))
            if not api or not secret:
                raise ValueError("configured credential environment variables are missing or empty")
        if api or secret:
            if not api or not secret:
                raise ValueError("api_key and secret_key must be configured together")
            if source.get("key_type_env"):
                key_type = os.getenv(str(source["key_type_env"]), "").lower()
                if not key_type:
                    raise ValueError("configured key_type_env is missing or empty")
            return str(api), str(secret), key_type
    return fallback or (None, None, "auto")
