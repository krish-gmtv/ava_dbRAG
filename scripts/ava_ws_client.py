import json
import time
from typing import Any, Dict, List, Optional, Tuple


try:
    import websocket  # websocket-client
except ImportError:  # pragma: no cover
    websocket = None  # type: ignore


WS_URL_TEMPLATE = "wss://ava.andrew-chat.com/api/v1/stream?token={token}"
END_MARKERS = ["<<END_OF_RESPONSE>>", "<END_OF_RESPONSE>", "[DONE]"]


def _strip_end_markers(text: str) -> str:
    cleaned = text
    for m in END_MARKERS:
        cleaned = cleaned.replace(m, "")
    return cleaned


def _has_end_marker(text: str) -> bool:
    return any(m in text for m in END_MARKERS)


def parse_ws_frame(frame: Any) -> Dict[str, Any]:
    """
    Ava stream response schema isn't explicitly given in the PDF for frames.
    This function uses heuristics:
    - If frame is JSON, attempt to find common text-ish fields.
    - Otherwise treat it as raw text.
    """
    if frame is None:
        return {"raw": None, "text": None, "is_error": False, "error_message": None}
    if isinstance(frame, (bytes, bytearray)):
        try:
            frame = frame.decode("utf-8", errors="ignore")
        except Exception:
            return {"raw": None, "text": None, "is_error": True, "error_message": "Unable to decode frame bytes."}

    if not isinstance(frame, str):
        return {"raw": None, "text": None, "is_error": False, "error_message": None}

    s = frame.strip()
    if not s:
        return {"raw": s, "text": None, "is_error": False, "error_message": None}

    # Try JSON parse first
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            # Detect explicit error/event payloads early.
            if isinstance(obj.get("error"), str) and obj.get("error").strip():
                return {
                    "raw": s,
                    "text": None,
                    "is_error": True,
                    "error_message": obj.get("error").strip(),
                }
            if isinstance(obj.get("type"), str) and obj.get("type").lower() in {"error", "failed"}:
                msg = (
                    obj.get("message")
                    or obj.get("detail")
                    or obj.get("error")
                    or "WS server returned error frame."
                )
                return {
                    "raw": s,
                    "text": None,
                    "is_error": True,
                    "error_message": str(msg),
                }
            for key in ("text", "message", "content", "delta", "chunk", "response"):
                v = obj.get(key)
                if isinstance(v, str) and v.strip():
                    return {
                        "raw": s,
                        "text": v.strip(),
                        "is_error": False,
                        "error_message": None,
                    }
            # Some APIs wrap streaming deltas under nested fields.
            if isinstance(obj.get("data"), dict):
                for key in ("text", "message", "content", "delta", "chunk"):
                    v = obj["data"].get(key)
                    if isinstance(v, str) and v.strip():
                        return {
                            "raw": s,
                            "text": v.strip(),
                            "is_error": False,
                            "error_message": None,
                        }
            return {"raw": s, "text": None, "is_error": False, "error_message": None}
        except json.JSONDecodeError:
            return {"raw": s, "text": s, "is_error": False, "error_message": None}
    return {"raw": s, "text": s, "is_error": False, "error_message": None}


def stream_chat_text(
    token: str,
    user_id: str,
    session_id: str,
    message: str,
    receive_timeout_sec: float,
    max_idle_sec: float = 10.0,
) -> Tuple[str, List[str]]:
    if websocket is None:
        raise RuntimeError("Missing dependency websocket-client. Install with: pip install websocket-client")

    ws_url = WS_URL_TEMPLATE.format(token=token)
    ws = websocket.create_connection(ws_url, timeout=receive_timeout_sec)

    try:
        payload = {"user_id": user_id, "session_id": session_id, "message": message}
        ws.send(json.dumps(payload))

        frames: List[str] = []
        chunks: List[str] = []
        start = time.time()
        last_frame_at = time.time()
        saw_any_frame = False

        # Collect frames until we hit total timeout or idle timeout.
        while True:
            elapsed = time.time() - start
            idle = time.time() - last_frame_at
            if elapsed > receive_timeout_sec or idle > max_idle_sec:
                break

            try:
                frame = ws.recv()
            except Exception as exc:
                # Distinguish likely timeout/close from unexpected transport error.
                exc_name = type(exc).__name__.lower()
                exc_msg = str(exc).lower()
                if "timeout" in exc_name or "timed out" in exc_msg:
                    # Treat read timeout as end-of-stream if we already collected text.
                    if chunks:
                        break
                    raise RuntimeError("WS receive timeout before any text was produced.") from exc
                if "closed" in exc_name or "close" in exc_msg:
                    # Connection closed can be normal after stream completion.
                    if chunks:
                        break
                    raise RuntimeError("WS connection closed before any text was produced.") from exc
                raise RuntimeError(f"WS receive error: {exc}") from exc

            if frame is None:
                continue
            frame_str = str(frame)
            if frame_str.strip():
                saw_any_frame = True
                frames.append(frame_str)
                last_frame_at = time.time()
                parsed = parse_ws_frame(frame)
                if parsed.get("is_error"):
                    raise RuntimeError(f"WS error frame received: {parsed.get('error_message')}")
                t = parsed.get("text")
                if isinstance(t, str) and t:
                    if _has_end_marker(t):
                        chunks.append(_strip_end_markers(t))
                        break
                    chunks.append(t)

        full_text = _strip_end_markers("".join(chunks)).strip() if chunks else ""
        if saw_any_frame and not full_text:
            raise RuntimeError("WS returned frames but no parsable text chunks.")
        if not saw_any_frame:
            raise RuntimeError("WS returned no frames.")
        return full_text, frames
    finally:
        try:
            ws.close()
        except Exception:
            pass

