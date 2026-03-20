import argparse
import json
import os
import time
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

try:
    import websocket  # websocket-client
except ImportError:  # pragma: no cover
    websocket = None  # type: ignore


load_dotenv()

AUTH_URL = "https://ava.andrew-chat.com/api/v1/user"
PRISM_SESSION_URL_TEMPLATE = "https://prism.andrew-chat.com/api/v1/prism/get_session/{user_id}/ava"
AVA_CLOSE_SESSION_URL_TEMPLATE = "https://ava.andrew-chat.com/api/v1/session/{user_id}"
AVA_STREAM_WS_URL_TEMPLATE = "wss://ava.andrew-chat.com/api/v1/stream?token={token}"


def get_token_from_auth(username: str, password: str) -> str:
    resp = requests.post(
        AUTH_URL,
        headers={"Content-Type": "application/json"},
        json={"username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    token = (data.get("authorization") or "").strip()
    if not token:
        raise RuntimeError("Auth response missing 'authorization' token.")
    return token


def get_session_id(user_id: str, token: str) -> str:
    url = PRISM_SESSION_URL_TEMPLATE.format(user_id=user_id)
    resp = requests.get(
        url,
        headers={"Authorization": token},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    session_id = (data.get("session_id") or "").strip()
    if not session_id:
        raise RuntimeError("Session response missing 'session_id'.")
    return session_id


def close_session(user_id: str, session_id: str, token: str) -> None:
    """
    Optional cleanup call. Safe to skip for smoke tests.
    """
    url = AVA_CLOSE_SESSION_URL_TEMPLATE.format(user_id=user_id)
    resp = requests.post(
        url,
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={"session_id": session_id},
        timeout=30,
    )
    # We keep this best-effort; not all deployments expose same close endpoint behavior.
    if resp.status_code >= 400:
        print(f"[WARN] close session returned {resp.status_code}: {resp.text[:300]}")
    else:
        print("[INFO] session close request sent successfully.")


def ws_send_and_receive(
    token: str,
    user_id: str,
    session_id: str,
    message: str,
    receive_timeout_sec: float,
) -> Dict[str, Any]:
    if websocket is None:
        raise RuntimeError(
            "Missing dependency 'websocket-client'. Install with: pip install websocket-client"
        )

    ws_url = AVA_STREAM_WS_URL_TEMPLATE.format(token=token)
    ws = websocket.create_connection(ws_url, timeout=receive_timeout_sec)
    try:
        payload = {
            "user_id": user_id,
            "session_id": session_id,
            "message": message,
        }
        ws.send(json.dumps(payload))
        print(f"[INFO] sent WS message payload: {payload}")

        # Collect a few frames until timeout/no more data.
        frames = []
        start = time.time()
        while True:
            elapsed = time.time() - start
            if elapsed > receive_timeout_sec:
                break
            try:
                frame = ws.recv()
            except Exception:
                break
            if frame is None:
                break
            frame_str = str(frame)
            if frame_str.strip():
                frames.append(frame_str)

        return {
            "ws_url": ws_url,
            "sent_message": message,
            "received_count": len(frames),
            "received_frames": frames,
        }
    finally:
        ws.close()


def resolve_token(args: argparse.Namespace) -> str:
    if args.token:
        return args.token
    env_token = (os.environ.get("AVA_TOKEN") or "").strip()
    if env_token:
        return env_token

    username = (args.username or os.environ.get("AVA_USERNAME") or "").strip()
    password = (args.password or os.environ.get("AVA_PASSWORD") or "").strip()
    if username and password:
        print("[INFO] AVA_TOKEN not provided; requesting token via username/password auth...")
        return get_token_from_auth(username, password)

    raise SystemExit(
        "No token available. Provide --token, or set AVA_TOKEN, "
        "or provide --username/--password (or AVA_USERNAME/AVA_PASSWORD)."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Standalone Ava websocket smoke test:\n"
            "1) Resolve token\n"
            "2) Get session_id\n"
            "3) Open websocket and send a message\n"
            "4) Print raw received frames"
        )
    )
    parser.add_argument("--user-id", type=str, default="smoke-user-001")
    parser.add_argument("--token", type=str, default="")
    parser.add_argument("--username", type=str, default="")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument("--message", type=str, default="Hello!")
    parser.add_argument("--receive-timeout-sec", type=float, default=10.0)
    parser.add_argument("--close-session", action="store_true")
    args = parser.parse_args()

    token = resolve_token(args)
    print("[INFO] token resolved.")

    session_id = get_session_id(args.user_id, token)
    print(f"[INFO] session_id acquired: {session_id}")

    ws_result = ws_send_and_receive(
        token=token,
        user_id=args.user_id,
        session_id=session_id,
        message=args.message,
        receive_timeout_sec=args.receive_timeout_sec,
    )

    result = {
        "ok": True,
        "user_id": args.user_id,
        "session_id": session_id,
        "ws_result": ws_result,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if args.close_session:
        close_session(args.user_id, session_id, token)


if __name__ == "__main__":
    main()

