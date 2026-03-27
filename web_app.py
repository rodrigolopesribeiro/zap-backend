#!/usr/bin/env python3
from __future__ import annotations

import argparse
from typing import Optional

from flask import Flask, jsonify, render_template, request

from find_zap_by_address import DEFAULT_ADDRESS, DEFAULT_URL, FIXED_MAX_PAGES, parse_bool, run_search, validate_recent_days

app = Flask(__name__)


def sanitize_text(value: object) -> str:
    text = str(value or "").strip()
    if text.startswith("="):
        text = text.lstrip("= ").strip()
    return text


def parse_optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise ValueError("valor decimal nao permitido")
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def collect_payload_sources(payload: dict) -> list[dict]:
    sources: list[dict] = []
    if isinstance(payload, dict) and payload:
        sources.append(payload)
        body = payload.get("body")
        if isinstance(body, dict) and body:
            sources.append(body)

    if request.form:
        sources.append(request.form.to_dict())
    if request.args:
        sources.append(request.args.to_dict())
    return sources


def read_first_value(sources: list[dict], *keys: str) -> Optional[object]:
    for source in sources:
        if not source:
            continue
        for key in keys:
            if key not in source:
                continue
            value = source.get(key)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return None


def build_validation_error_response(message: str, start_page: Optional[int] = None):
    resolved_start_page = start_page if isinstance(start_page, int) and start_page >= 1 else 1
    payload = {
        "status": "error",
        "start_page": resolved_start_page,
        "fixed_max_pages": FIXED_MAX_PAGES,
        "pages_processed": 0,
        "last_page_processed": 0,
        "next_start_page": None,
        "max_page_hint": None,
        "has_more_pages": False,
        "stop_reason": "validation_error",
        "total_matches": 0,
        "matches": [],
        "visited_properties": 0,
        "elapsed_seconds": 0.0,
        "error_message": message,
        "error": message,
        "logs": [f"[erro-validacao] {message}"],
        # Campos legados por compatibilidade com frontend atual.
        "url": None,
        "urls": [],
        "matches_count": 0,
        "pages_scanned": 0,
        "matches_detail": [],
    }
    return jsonify(payload), 400


@app.get("/")
def index():
    return render_template(
        "index.html",
        default_url=DEFAULT_URL,
        default_address=DEFAULT_ADDRESS,
    )


@app.post("/api/search")
def api_search():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        payload = {}

    sources = collect_payload_sources(payload)

    raw_start_page = read_first_value(
        sources,
        "start_page",
        "startPage",
        "pagina",
        "page",
    )
    start_page_source = "start_page"
    if raw_start_page is None:
        raw_start_page = read_first_value(sources, "next_start_page", "nextStartPage")
        if raw_start_page is not None:
            start_page_source = "next_start_page"

    if raw_start_page is None:
        start_page = 1
    else:
        try:
            parsed = parse_optional_int(raw_start_page)
        except ValueError:
            return build_validation_error_response("start_page deve ser um inteiro >= 1")
        if parsed is None or parsed < 1:
            return build_validation_error_response("start_page deve ser um inteiro >= 1")
        start_page = parsed

    listing_url = sanitize_text(
        read_first_value(sources, "listing_url", "listingUrl", "url", "listing") or ""
    )
    address = sanitize_text(read_first_value(sources, "address", "address_input") or "")
    condominium = sanitize_text(
        read_first_value(sources, "condominium", "condominio", "condominium_input") or ""
    )
    if not listing_url:
        return build_validation_error_response("listing_url (ou url) e obrigatorio", start_page=start_page)

    try:
        raw_headless = read_first_value(sources, "headless")
        headless = parse_bool(str(raw_headless if raw_headless is not None else "true"))
    except Exception as exc:  # noqa: BLE001
        return build_validation_error_response(f"Valor invalido para headless: {exc}", start_page=start_page)

    try:
        recent_days = parse_optional_int(read_first_value(sources, "recent_days", "recentDays"))
        timeout = parse_optional_int(read_first_value(sources, "timeout", "timeout_ms", "timeoutMs")) or 30000
    except ValueError:
        return build_validation_error_response(
            "recent_days/timeout devem ser inteiros.",
            start_page=start_page,
        )

    if timeout < 1000:
        return build_validation_error_response("timeout deve ser >= 1000 ms.", start_page=start_page)
    if start_page < 1:
        return build_validation_error_response("start_page deve ser um inteiro >= 1", start_page=start_page)
    try:
        recent_days = validate_recent_days(recent_days)
    except ValueError as exc:
        return build_validation_error_response(str(exc), start_page=start_page)
    if not address and not condominium and recent_days is None:
        return build_validation_error_response(
            "Informe ao menos um criterio: endereco (parcial), condominio ou criado nos ultimos (dias).",
            start_page=start_page,
        )

    def emit_log(message: str) -> None:
        print(message, flush=True)

    api_logs: list[str] = []
    if raw_start_page is None:
        api_logs.append("[api] start_page ausente no request, usando padrao=1")
    else:
        api_logs.append(f"[api] start_page recebido=({start_page_source}) {raw_start_page!r}")
    api_logs.append(f"[api] start_page efetivo={start_page}")
    api_logs.append(f"[api] fixed_max_pages efetivo={FIXED_MAX_PAGES}")
    for line in api_logs:
        print(line, flush=True)

    result = run_search(
        listing_url=listing_url,
        target_address=address,
        condominium=condominium,
        headless=headless,
        timeout=timeout,
        start_page=start_page,
        recent_days=recent_days,
        log_fn=emit_log,
    )
    for line in reversed(api_logs):
        result.logs.insert(0, line)
    return jsonify(result.api_payload()), 200


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Frontend local para busca de imoveis do Zap.")
    parser.add_argument("--host", default="127.0.0.1", help="Host do servidor Flask.")
    parser.add_argument("--port", default=5000, type=int, help="Porta do servidor Flask.")
    parser.add_argument("--debug", default="false", type=parse_bool, help="true/false para debug.")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)
