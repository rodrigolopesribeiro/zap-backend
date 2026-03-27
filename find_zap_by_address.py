#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable, Optional, Set
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Error,
    Page,
    Playwright,
    TimeoutError,
    sync_playwright,
)

DEFAULT_URL = (
    "https://www.zapimoveis.com.br/venda/apartamentos/rj+rio-de-janeiro/"
    "avenida-lucio-costa/?transacao=venda&onde=%2CRio+de+Janeiro%2CRio+de+Janeiro%2C"
    "Zona+Oeste%2CBarra+da+Tijuca%2CAvenida+L%C3%BAcio+Costa%2C%2Cstreet%2CBR%3ERio+de+Janeiro"
    "%3ENULL%3ERio+de+Janeiro%3EZona+Oeste%3EBarra+da+Tijuca%2C-23.011213%2C-43.372959%2C&"
    "tipos=apartamento_residencial"
)
DEFAULT_ADDRESS = "Avenida Lucio Costa, 3604 - Barra da Tijuca, Rio de Janeiro - RJ"
FIXED_MAX_PAGES = 2  # Limite fixo para reduzir bloqueio anti-bot.

LISTING_CARD_SELECTORS = (
    'li[data-cy="rp-property-cd"]',
    'article[data-cy*="property"]',
    'div[data-cy*="property-card"]',
    "li[data-position]",
    "article[class*='card']",
)
LISTING_LINK_SELECTOR = 'a[href*="/imovel/"]'
LOCATION_KEYWORDS = ("localizacao", "localizacao:")
ADDRESS_HINTS = (
    "avenida",
    "av.",
    "rua",
    "estrada",
    "travessa",
    "alameda",
    "rodovia",
    "praca",
    "largo",
    "rio de janeiro",
    "barra da tijuca",
)
ACCEPT_COOKIE_SELECTORS = (
    'button:has-text("Aceitar")',
    'button:has-text("Concordo")',
    'button:has-text("OK")',
    '[id*="accept" i]',
    '[data-testid*="accept" i]',
)
NEXT_PAGE_SELECTORS = (
    'a.olx-core-pagination__icon[href*="pagina=" i]',
    'a.olx-core-pagination__button[href*="pagina=" i]',
    'a[aria-label*="proxima pagina" i], a[aria-label*="próxima página" i]',
    'a[href*="pagina=" i][aria-label*="proxima" i], a[href*="pagina=" i][aria-label*="próxima" i]',
    'a[rel="next"]',
    'a[href*="next" i], a[data-testid*="next" i]',
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
BROWSER_ARGS = (
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process",
    "--no-first-run",
    "--disable-dev-shm-usage",
)
BLOCKED_TITLE_HINTS = (
    "attention required",
    "just a moment",
    "cloudflare",
    "acesso negado",
)
BLOCKED_BODY_HINTS = (
    "cloudflare",
    "attention required",
    "just a moment",
    "checking your browser",
    "verify you are human",
    "captcha",
    "acesso negado",
    "desculpe",
    "ray id",
    "incompatibilidade da extensao do navegador",
    "incompatibilidade da extensão do navegador",
)
MAX_CONSECUTIVE_BLOCKED_DETAILS = 3
MANUAL_UNBLOCK_WAIT_SECONDS = 45
MAX_TOTAL_BLOCKED_DETAILS = 12
PRE_PROPERTY_DELAY_MS = (550, 1500)
PAGE_CHANGE_DELAY_MS = (750, 1700)
PERIODIC_COOLDOWN_EVERY = 10
PERIODIC_COOLDOWN_MS = (5000, 9500)
BLOCK_COOLDOWN_MS = (9000, 17000)
MONTH_PTBR_TO_NUMBER = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}
CREATED_DATE_REGEX = re.compile(
    r"an[úu]ncio criado em\s+(\d{1,2})\s+de\s+([a-zA-ZÀ-ÖØ-öø-ÿ]+)\s+de\s+(\d{4})",
    re.IGNORECASE,
)
LISTING_ID_REGEX = re.compile(r"-id-(\d+)", re.IGNORECASE)

LogFn = Callable[[str], None]


class BlockedPageError(RuntimeError):
    pass


@dataclass
class SearchResult:
    status: str
    start_page: int
    fixed_max_pages: int
    pages_processed: int
    last_page_processed: int
    next_start_page: Optional[int]
    max_page_hint: Optional[int]
    has_more_pages: bool
    stop_reason: str
    visited_properties: int
    elapsed_seconds: float
    total_matches: int
    matches: list[dict[str, object]] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)
    error_message: Optional[str] = None

    @property
    def url(self) -> Optional[str]:
        urls = self.property_urls
        return urls[0] if urls else None

    @property
    def property_urls(self) -> list[str]:
        urls: list[str] = []
        for item in self.matches:
            url = item.get("property_url")
            if url:
                urls.append(str(url))
        return urls

    def _payload(self) -> dict:
        payload = {
            "status": self.status,
            "start_page": self.start_page,
            "fixed_max_pages": self.fixed_max_pages,
            "pages_processed": self.pages_processed,
            "pages_scanned": self.pages_processed,
            "last_page_processed": self.last_page_processed,
            "next_start_page": self.next_start_page,
            "max_page_hint": self.max_page_hint,
            "page_hint_max": self.max_page_hint,
            "pages_hint": self.max_page_hint,
            "has_more_pages": self.has_more_pages,
            "stop_reason": self.stop_reason,
            "total_matches": self.total_matches,
            "matches": self.matches,
            "visited_properties": self.visited_properties,
            "elapsed_seconds": self.elapsed_seconds,
            "finished": (not self.has_more_pages)
            and self.stop_reason in {"reached_last_page", "no_next_page"},
            "blocked": self.stop_reason == "blocked_or_failed",
            "logs": self.logs,
        }
        legacy_matches_detail: list[dict[str, object]] = []
        for match in self.matches:
            legacy_matches_detail.append(
                {
                    "url": match.get("property_url"),
                    "created_date": None,
                    "endereco_extraido": match.get("address_extracted"),
                    "address_matched": match.get("address_match"),
                    "condominium_matched": match.get("condominium_match"),
                    "condominium_fragment": None,
                }
            )
        payload["url"] = self.url
        payload["urls"] = self.property_urls
        payload["matches_count"] = self.total_matches
        payload["pages_scanned"] = self.pages_processed
        payload["matches_detail"] = legacy_matches_detail
        if self.error_message:
            payload["error_message"] = self.error_message
            payload["error"] = self.error_message
        return payload

    def cli_payload(self) -> dict:
        return self._payload()

    def api_payload(self) -> dict:
        return self._payload()


def parse_bool(value: str) -> bool:
    parsed = str(value).strip().lower()
    if parsed in {"1", "true", "t", "yes", "y", "sim", "s"}:
        return True
    if parsed in {"0", "false", "f", "no", "n", "nao", "nao"}:
        return False
    raise argparse.ArgumentTypeError("Use true/false para --headless.")


def validate_recent_days(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if value < 0 or value > 30:
        raise ValueError("recent_days deve ser um inteiro entre 0 e 30.")
    return value


def normalize(text: str) -> str:
    if text is None:
        return ""
    text = text.replace("\xa0", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_for_contains(text: str) -> str:
    text = normalize(text)
    text = re.sub(r"[^\w]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_spaces_only(text: str) -> str:
    return normalize(text)


def compile_condominium_regex(query: str) -> tuple[Optional[re.Pattern[str]], str]:
    query_norm = normalize_spaces_only(query)
    if not query_norm:
        return None, ""
    parts = [part for part in query_norm.split(" ") if part]
    if not parts:
        return None, query_norm
    inner_pattern = r"\s+".join(re.escape(part) for part in parts)
    pattern = re.compile(rf"(^|[^\w])({inner_pattern})($|[^\w])")
    return pattern, query_norm


def condominium_match_context(text: str, start: int, end: int, radius: int = 60) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    snippet = text[left:right].strip()
    return re.sub(r"\s+", " ", snippet)


def is_probable_condo_list_context(text: str, match_start: int, match_end: int) -> bool:
    before = text[max(0, match_start - 80):match_start]
    after = text[match_end:match_end + 80]
    after_lstrip = after.lstrip()
    after_first_60 = after_lstrip[:60]
    separator_count = sum(after_first_60.count(symbol) for symbol in [",", "/", ";"])

    if after_lstrip.startswith("/") or after_lstrip.startswith(";"):
        return True

    if separator_count >= 2:
        return True

    if after_lstrip.startswith(","):
        if any(symbol in after_lstrip[1:50] for symbol in [",", "/", ";"]):
            return True
        if re.search(r"\b(condominios?|tambem temos|outros|opcoes?)\b", before):
            return True

    return False


def match_condominium_in_description(
    description_text: Optional[str],
    condo_query: str,
) -> tuple[bool, Optional[str], str]:
    if not description_text:
        return False, None, "descricao indisponivel"

    regex, query_norm = compile_condominium_regex(condo_query)
    if not regex or not query_norm:
        return False, None, "query de condominio vazia apos normalizacao"

    normalized_description = normalize_spaces_only(description_text)
    matched = regex.search(normalized_description)
    if not matched:
        return False, None, "termo de condominio nao encontrado com fronteiras"

    match_start, match_end = matched.start(2), matched.end(2)
    if is_probable_condo_list_context(normalized_description, match_start, match_end):
        context = condominium_match_context(normalized_description, match_start, match_end)
        return False, context, "descartado por provavel lista de condominios"

    context = condominium_match_context(normalized_description, match_start, match_end)
    return True, context, ""


def default_log(message: str) -> None:
    print(message, flush=True)


def is_closed_target_error(exc: Exception) -> bool:
    return "has been closed" in str(exc).lower()


def pause_with_jitter(
    page: Page,
    delay_range_ms: tuple[int, int],
    log_fn: Optional[LogFn] = None,
    reason: Optional[str] = None,
) -> None:
    start_ms, end_ms = delay_range_ms
    wait_ms = random.randint(start_ms, end_ms)
    if log_fn and reason:
        log_fn(f"[ritmo] Pausa {wait_ms}ms ({reason}).")
    page.wait_for_timeout(wait_ms)


def safe_page_title(page: Page) -> str:
    try:
        return page.title()
    except Error:
        return ""


def is_probably_blocked_page(page: Page) -> tuple[bool, str]:
    title_raw = safe_page_title(page)
    title_norm = normalize(title_raw)
    if any(hint in title_norm for hint in BLOCKED_TITLE_HINTS):
        return True, f"title={title_raw!r}"

    body_text = ""
    try:
        body_text = page.locator("body").inner_text(timeout=1700)
    except Error:
        body_text = ""
    body_norm = normalize(body_text[:7000])
    if any(hint in body_norm for hint in BLOCKED_BODY_HINTS):
        return True, "conteudo com sinais de anti-bot/captcha"
    return False, ""


def wait_for_manual_unblock(page: Page, log_fn: LogFn, max_wait_seconds: int) -> bool:
    if max_wait_seconds <= 0:
        return False
    log_fn(
        "[acao] Desafio anti-bot detectado. Resolva manualmente no navegador aberto "
        f"(ate {max_wait_seconds}s)."
    )
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            page.wait_for_timeout(1200)
        except Error:
            return False
        blocked, _ = is_probably_blocked_page(page)
        if not blocked:
            log_fn("[acao] Desafio resolvido. Retomando automacao.")
            return True
    log_fn("[acao] Tempo de espera para desbloqueio manual esgotado.")
    return False


def open_with_retries(
    page: Page,
    url: str,
    timeout: int,
    retries: int,
    log_fn: LogFn,
    allow_manual_unblock: bool = False,
    manual_unblock_wait_seconds: int = MANUAL_UNBLOCK_WAIT_SECONDS,
) -> None:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            log_fn(f"[retry] Abrindo {url} (tentativa {attempt}/{retries})")
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            blocked, block_reason = is_probably_blocked_page(page)
            if blocked:
                if allow_manual_unblock and wait_for_manual_unblock(
                    page,
                    log_fn=log_fn,
                    max_wait_seconds=manual_unblock_wait_seconds,
                ):
                    return
                raise BlockedPageError(f"pagina bloqueada ({block_reason})")
            return
        except (TimeoutError, Error, BlockedPageError) as exc:
            last_error = exc
            if is_closed_target_error(exc):
                raise
            wait_ms = min(900 * attempt, 3000) + random.randint(120, 520)
            if isinstance(exc, BlockedPageError):
                log_fn(f"[bloqueio] {exc}. Novo retry em {wait_ms}ms.")
            try:
                page.wait_for_timeout(wait_ms)
            except Error:
                raise
    if last_error:
        raise last_error


def dismiss_cookie_banner(page: Page, timeout: int, log_fn: LogFn) -> None:
    partial_timeout = max(600, int(timeout * 0.08))
    for selector in ACCEPT_COOKIE_SELECTORS:
        locator = page.locator(selector).first
        try:
            if locator.count() == 0:
                continue
            if not locator.is_visible():
                continue
            locator.click(timeout=partial_timeout)
            log_fn(f"[cookie] Banner tratado com seletor: {selector}")
            page.wait_for_timeout(200)
            return
        except Error:
            continue


def detect_card_selector(page: Page, timeout: int) -> Optional[str]:
    partial_timeout = max(1500, int(timeout / (len(LISTING_CARD_SELECTORS) + 1)))
    for selector in LISTING_CARD_SELECTORS:
        try:
            page.wait_for_selector(selector, timeout=partial_timeout)
            return selector
        except TimeoutError:
            continue
    return None


def warm_listing_page(page: Page, timeout: int, log_fn: LogFn) -> Optional[str]:
    dismiss_cookie_banner(page, timeout=timeout, log_fn=log_fn)
    selector = detect_card_selector(page, timeout=timeout)
    if selector:
        return selector

    for pass_index in range(1, 4):
        try:
            page.wait_for_load_state("networkidle", timeout=max(1200, int(timeout * 0.25)))
        except TimeoutError:
            pass
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(220)
        page.mouse.wheel(0, -400)
        page.wait_for_timeout(150)
        selector = detect_card_selector(page, timeout=max(1600, int(timeout * 0.2)))
        if selector:
            log_fn(f"[listagem] Cards apareceram apos aquecimento pass={pass_index}.")
            return selector
        has_links = page.locator(LISTING_LINK_SELECTOR).count() > 0
        if has_links:
            log_fn(f"[listagem] Links encontrados por fallback pass={pass_index}.")
            return None
    return None


def collect_listing_links(page: Page, card_selector: Optional[str]) -> Set[str]:
    links: Set[str] = set()
    if card_selector:
        cards = page.query_selector_all(card_selector)
        for card in cards:
            anchor = card.query_selector(LISTING_LINK_SELECTOR)
            if not anchor:
                continue
            href = anchor.get_attribute("href")
            if not href:
                continue
            links.add(urljoin(page.url, href))

    if links:
        return links

    anchors = page.query_selector_all(LISTING_LINK_SELECTOR)
    for anchor in anchors:
        href = anchor.get_attribute("href")
        if not href:
            continue
        links.add(urljoin(page.url, href))
    return links


def parse_created_date_from_text(text: str) -> tuple[Optional[date], Optional[str], str]:
    raw = (text or "").strip()
    if not raw:
        return None, None, "texto vazio"

    match = CREATED_DATE_REGEX.search(raw)
    if not match:
        return None, None, "trecho 'Anuncio criado em ...' nao encontrado"

    day_str, month_label_raw, year_str = match.groups()
    created_fragment = match.group(0).strip()
    month_key = normalize(month_label_raw)
    month_number = MONTH_PTBR_TO_NUMBER.get(month_key)
    if month_number is None:
        return None, created_fragment, f"mes invalido: {month_label_raw!r}"

    try:
        created = date(int(year_str), month_number, int(day_str))
    except ValueError as exc:
        return None, created_fragment, f"data invalida: {exc}"

    return created, created_fragment, ""


def walk_json_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from walk_json_nodes(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from walk_json_nodes(nested)


def load_json_ld_objects(page: Page) -> list[Any]:
    objects: list[Any] = []
    scripts = page.query_selector_all('script[type="application/ld+json"]')
    for script in scripts[:20]:
        try:
            raw = (script.inner_text() or "").strip()
        except Error:
            continue
        if not raw:
            continue
        try:
            objects.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return objects


def extract_address_from_json_ld(page: Page) -> Optional[str]:
    for payload in load_json_ld_objects(page):
        for node in walk_json_nodes(payload):
            if not isinstance(node, dict):
                continue

            address = node.get("address")
            if isinstance(address, str) and line_looks_like_address(address):
                return address.strip()
            if isinstance(address, dict):
                street = str(address.get("streetAddress", "")).strip()
                locality = str(address.get("addressLocality", "")).strip()
                region = str(address.get("addressRegion", "")).strip()
                if street:
                    tail = ", ".join(part for part in (locality, region) if part)
                    return f"{street} - {tail}" if tail else street

            street = node.get("streetAddress")
            if isinstance(street, str) and line_looks_like_address(street):
                return street.strip()
    return None


def extract_description_from_json_ld(page: Page) -> Optional[str]:
    for payload in load_json_ld_objects(page):
        for node in walk_json_nodes(payload):
            if not isinstance(node, dict):
                continue
            description = node.get("description")
            if not isinstance(description, str):
                continue
            text = description.strip()
            if len(normalize(text)) >= 12:
                return text
    return None


def extract_property_created_date(page: Page) -> tuple[Optional[date], Optional[str], str]:
    locator = page.locator('span[data-testid="listing-created-date"]').first
    try:
        if locator.count() == 0:
            return None, None, "seletor listing-created-date nao encontrado"
        created_text = (locator.inner_text(timeout=2000) or "").strip()
    except Error as exc:
        return None, None, f"erro ao ler listing-created-date: {exc}"

    parsed_date, fragment, reason = parse_created_date_from_text(created_text)
    if parsed_date is None:
        return None, fragment, f"{reason} | texto={created_text!r}"
    return parsed_date, fragment, ""


def listing_signature(page: Page) -> str:
    try:
        links = sorted(collect_listing_links(page, card_selector=None))
    except Error:
        return ""
    if not links:
        return ""
    return "|".join(links[:8])


def parse_page_number(url: str) -> int:
    query = dict(parse_qsl(urlparse(url).query, keep_blank_values=True))
    for key in ("pagina", "page", "p"):
        raw_value = str(query.get(key, "")).strip()
        if raw_value.isdigit():
            parsed = int(raw_value)
            if parsed > 0:
                return parsed
    return 1


def extract_listing_id(property_url: str) -> Optional[str]:
    if not property_url:
        return None
    match = LISTING_ID_REGEX.search(property_url)
    if match:
        return match.group(1)
    return None


def build_url_with_page(url: str, page_number: int) -> str:
    parsed = urlparse(url)
    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page_number <= 1:
        query_items.pop("pagina", None)
    else:
        query_items["pagina"] = str(page_number)
    new_query = urlencode(query_items, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def parse_page_from_text(value: str) -> Optional[int]:
    text = (value or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    parsed = int(digits)
    return parsed if parsed > 0 else None


def detect_max_page_hint(page: Page) -> Optional[int]:
    candidates = page.locator(
        'a.olx-core-pagination__button, a[aria-label*="página" i], a[aria-label*="pagina" i], a[href*="pagina=" i]'
    )
    max_page: Optional[int] = None
    try:
        count = candidates.count()
    except Error:
        return None

    for idx in range(min(count, 30)):
        locator = candidates.nth(idx)
        aria = locator.get_attribute("aria-label") or ""
        href = locator.get_attribute("href") or ""
        text = locator_text(locator)

        page_number = parse_page_from_text(text)
        if page_number is None:
            page_number = parse_page_from_text(aria)
        if page_number is None and href:
            page_number = parse_page_number(urljoin(page.url, href))

        if page_number is None:
            continue
        if max_page is None or page_number > max_page:
            max_page = page_number
    return max_page


def refresh_max_page_hint(page: Page, current_hint: Optional[int], log_fn: LogFn) -> Optional[int]:
    hint = detect_max_page_hint(page)
    if hint is None:
        return current_hint
    if current_hint is None or hint > current_hint:
        log_fn(f"[paginacao] Hint de pagina maxima detectado: {hint}")
        return hint
    return current_hint


def detect_has_next_page_candidate(page: Page, max_page_hint: Optional[int]) -> bool:
    current_page_number = parse_page_number(page.url)
    if max_page_hint is not None:
        return current_page_number < max_page_hint

    for selector in NEXT_PAGE_SELECTORS:
        try:
            locator_group = page.locator(selector)
            count = locator_group.count()
        except Error:
            continue
        for candidate_idx in range(min(count, 8)):
            locator = locator_group.nth(candidate_idx)
            if not locator.is_visible():
                continue
            if locator_disabled(locator):
                continue
            if not is_likely_pagination_candidate(locator):
                continue
            href = locator.get_attribute("href") or ""
            if href:
                next_url = urljoin(page.url, href)
                next_page_number = parse_page_number(next_url)
                if next_page_number > current_page_number:
                    return True
            aria = normalize(locator.get_attribute("aria-label") or "")
            text = normalize(locator_text(locator))
            if "proxima" in aria or "proxima" in text or "next" in aria or "next" in text:
                return True
    return False


def line_looks_like_address(text_line: str) -> bool:
    norm = normalize(text_line)
    if not norm:
        return False
    return any(hint in norm for hint in ADDRESS_HINTS)


def extract_address_from_location_block(page: Page) -> Optional[str]:
    selectors = (
        'section:has-text("Localizacao"), section:has-text("Localização")',
        'article:has-text("Localizacao"), article:has-text("Localização")',
        'div:has-text("Localizacao"), div:has-text("Localização")',
        '[data-testid*="location" i]',
        '[class*="location" i]',
        '[id*="location" i]',
    )
    for selector in selectors:
        blocks = page.locator(selector)
        try:
            count = min(blocks.count(), 12)
        except Error:
            count = 0
        for index in range(count):
            block = blocks.nth(index)
            try:
                block_text = (block.inner_text(timeout=1500) or "").strip()
            except Error:
                continue
            if not block_text:
                continue
            lines = [line.strip() for line in block_text.splitlines() if line.strip()]
            for line in lines:
                normalized_line = normalize(line)
                if any(keyword in normalized_line for keyword in LOCATION_KEYWORDS):
                    continue
                if line_looks_like_address(line):
                    return line
            if len(lines) > 1:
                return lines[1]
    return None


def extract_address_fallback(page: Page) -> Optional[str]:
    body_text = page.locator("body").inner_text(timeout=4000)
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    candidate: Optional[str] = None
    for line in lines:
        if not line_looks_like_address(line):
            continue
        if "," in line and "-" in line:
            return line
        if candidate is None:
            candidate = line
    return candidate


def extract_property_address(page: Page) -> Optional[str]:
    block_address = extract_address_from_location_block(page)
    if block_address:
        return block_address
    structured_address = extract_address_from_json_ld(page)
    if structured_address:
        return structured_address
    return extract_address_fallback(page)


def extract_property_description(page: Page) -> tuple[Optional[str], str]:
    selectors = (
        'p[data-testid="description-content"]',
        '[data-testid*="description-content" i]',
        'p[data-testid*="description" i]',
        '[data-testid*="description" i] p',
        ".description__content--text",
        ".description__content",
    )
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="attached", timeout=1200)
        except Error:
            pass
        try:
            if locator.count() == 0:
                continue
            text = (locator.inner_text(timeout=2000) or "").strip()
        except Error:
            continue
        if text:
            return text, ""
    structured_description = extract_description_from_json_ld(page)
    if structured_description:
        return structured_description, ""
    try:
        meta_description = (page.locator('meta[name="description"]').first.get_attribute("content") or "").strip()
        if meta_description:
            return meta_description, ""
    except Error:
        pass
    return None, "descricao do anuncio nao encontrada"


def locator_disabled(locator) -> bool:
    aria_disabled = (locator.get_attribute("aria-disabled") or "").lower()
    if aria_disabled == "true":
        return True
    classes = (locator.get_attribute("class") or "").lower()
    if "disabled" in classes:
        return True
    return False


def locator_text(locator) -> str:
    try:
        return (locator.inner_text(timeout=900) or "").strip()
    except Error:
        return ""


def is_likely_pagination_candidate(locator) -> bool:
    classes = (locator.get_attribute("class") or "").lower()
    href = (locator.get_attribute("href") or "").lower()
    aria = normalize(locator.get_attribute("aria-label") or "")
    text = normalize(locator_text(locator))
    combined = " ".join((classes, href, aria, text))

    if "carousel" in classes:
        return False
    if "anterior" in combined or "previous" in combined:
        return False
    if "pagination" in classes:
        return True
    if "pagina=" in href or "page=" in href:
        return True
    if "proxima pagina" in combined or "next page" in combined:
        return True
    return False


def goto_next_page(
    page: Page,
    timeout: int,
    log_fn: LogFn,
    max_page_hint: Optional[int],
) -> tuple[bool, Optional[str]]:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(180)
    dismiss_cookie_banner(page, timeout=timeout, log_fn=log_fn)

    current_url = page.url
    current_signature = listing_signature(page)
    current_page_number = parse_page_number(current_url)
    click_timeout = max(1200, int(timeout * 0.4))

    for index, selector in enumerate(NEXT_PAGE_SELECTORS, start=1):
        try:
            locator_group = page.locator(selector)
            count = locator_group.count()
            if count == 0:
                continue

            for candidate_idx in range(min(count, 8)):
                locator = locator_group.nth(candidate_idx)
                if not locator.is_visible():
                    continue
                if locator_disabled(locator):
                    continue
                if not is_likely_pagination_candidate(locator):
                    continue

                fallback_href = locator.get_attribute("href")
                next_url = urljoin(current_url, fallback_href) if fallback_href else ""
                if next_url:
                    next_page_number = parse_page_number(next_url)
                    if next_page_number <= current_page_number:
                        continue
                    if max_page_hint is not None and next_page_number > max_page_hint:
                        continue

                log_fn(
                    f"[paginacao] Tentando fallback #{index} (cand={candidate_idx + 1}) "
                    f"href={next_url or '<sem href>'}"
                )

                try:
                    if next_url and next_url != current_url:
                        open_with_retries(page, next_url, timeout=timeout, retries=2, log_fn=log_fn)
                    else:
                        locator.click(timeout=click_timeout)
                except Error:
                    if not next_url:
                        continue
                    try:
                        open_with_retries(page, next_url, timeout=timeout, retries=2, log_fn=log_fn)
                    except Error:
                        continue
                    except BlockedPageError:
                        continue

                selector_found = warm_listing_page(page, timeout=timeout, log_fn=log_fn)
                new_signature = listing_signature(page)
                new_page_number = parse_page_number(page.url)

                if page.url != current_url:
                    return True, selector_found
                if new_signature and new_signature != current_signature:
                    return True, selector_found
                if new_page_number > current_page_number:
                    return True, selector_found

                log_fn("[paginacao] Sem avanço real; tentando próximo fallback.")
        except Error:
            continue
        except TimeoutError:
            continue
        except BlockedPageError:
            continue

    # Fallback final: forca pagina=N quando clique nao avanca.
    forced_next_url = build_url_with_page(current_url, current_page_number + 1)
    if forced_next_url != current_url:
        if max_page_hint is not None and current_page_number >= max_page_hint:
            return False, None
        log_fn(f"[paginacao] Fallback por URL: {forced_next_url}")
        try:
            open_with_retries(page, forced_next_url, timeout=timeout, retries=2, log_fn=log_fn)
            selector_found = warm_listing_page(page, timeout=timeout, log_fn=log_fn)
            new_signature = listing_signature(page)
            if page.url != current_url or (new_signature and new_signature != current_signature):
                return True, selector_found
        except (Error, TimeoutError, BlockedPageError):
            pass

    return False, None


def new_context(browser: Browser) -> BrowserContext:
    context = browser.new_context(
        user_agent=DEFAULT_USER_AGENT,
        locale="pt-BR",
        timezone_id="America/Sao_Paulo",
        viewport={"width": 1440, "height": 900},
        extra_http_headers={
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    # Reduz alguns sinais triviais de automacao no navegador.
    context.add_init_script(
        """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
"""
    )
    return context


def launch_browser(playwright: Playwright, headless: bool) -> Browser:
    return playwright.chromium.launch(headless=headless, args=list(BROWSER_ARGS))


def find_property_url(
    browser: Browser,
    listing_url: str,
    target_address: Optional[str],
    condominium: Optional[str],
    headless: bool,
    timeout: int,
    recent_days: Optional[int],
    log_fn: LogFn,
    start_page: int = 1,
) -> SearchResult:
    context = new_context(browser)
    listing_page = context.new_page()
    detail_page: Optional[Page] = None
    visited_count = 0
    scanned_pages = 0
    seen_urls: Set[str] = set()
    matches: list[dict[str, object]] = []
    match_keys: Set[str] = set()
    error_message: Optional[str] = None
    blocked_streak = 0
    total_blocked_details = 0
    abort_run = False
    stop_reason = ""
    max_page_hint: Optional[int] = None
    address_query = (target_address or "").strip()
    condo_query = (condominium or "").strip()
    target_norm = normalize_for_contains(address_query)
    condo_norm = normalize_spaces_only(condo_query)
    address_active = bool(target_norm)
    condo_active = bool(condo_norm)
    recent_days_applied = recent_days is not None
    if start_page < 1:
        context.close()
        error_message = "start_page deve ser >= 1."
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=0,
            last_page_processed=0,
            next_start_page=None,
            max_page_hint=None,
            has_more_pages=False,
            stop_reason="invalid_input",
            visited_properties=0,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            error_message=error_message,
        )

    current_page_number = start_page
    last_page_processed = start_page - 1
    print(
        f"[checkpoint] start_page={start_page} fixed_max_pages={FIXED_MAX_PAGES}",
        flush=True,
    )
    log_fn(
        f"[checkpoint] start_page={start_page} fixed_max_pages={FIXED_MAX_PAGES}"
    )
    log_fn(f"[batch] start_page={start_page} fixed_max_pages={FIXED_MAX_PAGES}")
    log_fn(f"[batch] iniciando bloco na pagina {start_page}")
    log_fn(
        f"[batch] paginas previstas neste bloco: {start_page}..{start_page + FIXED_MAX_PAGES - 1}"
    )
    if address_active:
        log_fn(f"[filtro] Address ativo: {address_query!r}")
        if len(target_norm) < 6:
            log_fn("[aviso] Query de endereco curta (<6 caracteres) pode gerar falsos positivos.")
    if condo_active:
        log_fn(f"[filtro] Condominio ativo: {condo_query!r}")
    if not address_active and not condo_active and not recent_days_applied:
        context.close()
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=0,
            last_page_processed=start_page - 1,
            next_start_page=None,
            max_page_hint=None,
            has_more_pages=False,
            stop_reason="invalid_input",
            visited_properties=0,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            error_message="Informe ao menos um criterio: endereco (parcial), condominio ou dias recentes.",
        )
    if not address_active and not condo_active and recent_days_applied:
        log_fn("[filtro] Sem endereco/condominio. Aplicando apenas filtro de data (recent_days).")
    today = date.today()
    date_window: Optional[dict[str, str]] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    if recent_days_applied:
        window_start = today - timedelta(days=recent_days)
        window_end = today
        date_window = {"from": window_start.isoformat(), "to": window_end.isoformat()}
        log_fn(
            f"[filtro-data] Ativo recent_days={recent_days} "
            f"(janela: {date_window['from']}..{date_window['to']})"
        )

    listing_url_with_page = build_url_with_page(listing_url, start_page)
    log_fn(f"[batch] url inicial={listing_url_with_page}")
    can_continue = True
    try:
        open_with_retries(
            listing_page,
            listing_url_with_page,
            timeout=timeout,
            retries=3,
            log_fn=log_fn,
            allow_manual_unblock=not headless,
            manual_unblock_wait_seconds=MANUAL_UNBLOCK_WAIT_SECONDS * 2,
        )
    except BlockedPageError as exc:
        error_message = f"Bloqueio anti-bot na listagem. {exc}"
        stop_reason = "blocked_or_failed"
        log_fn(f"[fim] {error_message}")
        can_continue = False

    if not can_continue:
        context.close()
        has_more_pages = False
        next_start_page = None
        log_fn(f"[batch] last_page_processed={last_page_processed}")
        log_fn(f"[batch] next_start_page={next_start_page}")
        log_fn(f"[batch] has_more_pages={str(has_more_pages).lower()}")
        log_fn(f"[batch] stop_reason={stop_reason or 'blocked_or_failed'}")
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=scanned_pages,
            last_page_processed=last_page_processed,
            next_start_page=next_start_page,
            max_page_hint=max_page_hint,
            has_more_pages=has_more_pages,
            stop_reason=stop_reason or "blocked_or_failed",
            visited_properties=visited_count,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            error_message=error_message,
        )

    card_selector = warm_listing_page(listing_page, timeout=timeout, log_fn=log_fn)
    detail_page = context.new_page()
    current_page_number = max(1, parse_page_number(listing_page.url))
    log_fn(f"[batch] current_page_number inicial={current_page_number}")
    if current_page_number != start_page:
        log_fn(
            "[aviso] Pagina inicial divergente do start_page "
            f"(start_page={start_page}, url_atual={listing_page.url})"
        )
    while True:
        print(
            f"[checkpoint] loop scanned_pages={scanned_pages} "
            f"current_page_number={current_page_number} fixed_max_pages={FIXED_MAX_PAGES}",
            flush=True,
        )
        log_fn(
            f"[checkpoint] loop scanned_pages={scanned_pages} "
            f"current_page_number={current_page_number} fixed_max_pages={FIXED_MAX_PAGES}"
        )
        max_page_hint = refresh_max_page_hint(listing_page, max_page_hint, log_fn)
        if scanned_pages >= FIXED_MAX_PAGES:
            print(
                f"[checkpoint] limite fixo atingido antes de processar pagina: "
                f"scanned_pages={scanned_pages} fixed_max_pages={FIXED_MAX_PAGES}",
                flush=True,
            )
            log_fn(
                f"[checkpoint] limite fixo atingido antes de processar pagina: "
                f"scanned_pages={scanned_pages} fixed_max_pages={FIXED_MAX_PAGES}"
            )
            log_fn(f"[fim] Limite fixo de paginas={FIXED_MAX_PAGES} atingido.")
            stop_reason = "fixed_limit_reached"
            break

        blocked_listing, blocked_reason = is_probably_blocked_page(listing_page)
        if blocked_listing:
            log_fn(f"[bloqueio] Listagem bloqueada na pagina {current_page_number}: {blocked_reason}.")
            recovered_listing = False
            if not headless:
                recovered_listing = wait_for_manual_unblock(
                    listing_page,
                    log_fn=log_fn,
                    max_wait_seconds=MANUAL_UNBLOCK_WAIT_SECONDS * 2,
                )
            if not recovered_listing:
                pause_with_jitter(
                    listing_page,
                    BLOCK_COOLDOWN_MS,
                    log_fn=log_fn,
                    reason="cooldown por bloqueio da listagem",
                )
                blocked_listing, _ = is_probably_blocked_page(listing_page)
                recovered_listing = not blocked_listing
            if not recovered_listing:
                error_message = (
                    "Bloqueio anti-bot na listagem. Nao foi possivel recuperar a sessao automaticamente."
                )
                stop_reason = "blocked_or_failed"
                log_fn(f"[fim] {error_message}")
                break
            card_selector = warm_listing_page(listing_page, timeout=timeout, log_fn=log_fn)

        card_count = len(listing_page.query_selector_all(card_selector)) if card_selector else 0
        log_fn(
            f"[listagem] Pagina {current_page_number} | URL: {listing_page.url} "
            f"| cards: {card_count} | seletor: {card_selector or 'fallback-links'}"
        )

        links = sorted(collect_listing_links(listing_page, card_selector))
        if not links:
            page_title = safe_page_title(listing_page)
            blocked_listing, blocked_reason = is_probably_blocked_page(listing_page)
            suffix = f" | bloqueio: {blocked_reason}" if blocked_listing else ""
            log_fn(f"[listagem] Nenhum link capturado. title={page_title!r}{suffix}")

        for index, property_url in enumerate(links, start=1):
            if abort_run:
                break
            if property_url in seen_urls:
                log_fn(f"[skip] URL duplicada ignorada: {property_url}")
                continue

            if index > 1 and index % PERIODIC_COOLDOWN_EVERY == 0:
                pause_with_jitter(
                    listing_page,
                    PERIODIC_COOLDOWN_MS,
                    log_fn=log_fn,
                    reason=f"respiro anti-bloqueio apos {index - 1} imoveis na pagina",
                )

            if detail_page.is_closed():
                try:
                    detail_page = context.new_page()
                    log_fn("[sessao] Aba de detalhe foi reaberta.")
                except Error:
                    error_message = "Contexto/navegador fechado durante a busca."
                    log_fn(f"[fim] {error_message}")
                    abort_run = True
                    break

            seen_urls.add(property_url)
            visited_count += 1
            log_fn(f"[imovel] ({index}/{len(links)}) Visitando: {property_url}")
            try:
                pause_with_jitter(detail_page, PRE_PROPERTY_DELAY_MS)
                open_with_retries(
                    detail_page,
                    property_url,
                    timeout=timeout,
                    retries=3,
                    log_fn=log_fn,
                    allow_manual_unblock=not headless,
                    manual_unblock_wait_seconds=MANUAL_UNBLOCK_WAIT_SECONDS * 2,
                )
                detail_page.wait_for_load_state("domcontentloaded", timeout=max(1400, int(timeout * 0.5)))
                extracted_address = extract_property_address(detail_page)
                log_fn(f"[imovel] Endereco extraido: {extracted_address!r}")
                blocked_streak = 0

                address_match = False
                if address_active:
                    normalized_extracted_address = normalize_for_contains(extracted_address or "")
                    address_match = bool(target_norm and target_norm in normalized_extracted_address)
                log_fn(f"[imovel] Match endereco: {address_match}")

                condo_match = False
                condo_fragment: Optional[str] = None
                condo_context: Optional[str] = None
                condo_reason = ""
                if condo_active:
                    description_text, description_reason = extract_property_description(detail_page)
                    condo_match, condo_context, condo_reason = match_condominium_in_description(
                        description_text, condo_query
                    )
                    if condo_match:
                        condo_fragment = condo_context
                    else:
                        if "provavel lista" in condo_reason:
                            log_fn("[imovel] Match condominio descartado por provavel lista.")
                    extra_reason = condo_reason or description_reason
                    log_fn(f"[imovel] Match condominio: {condo_match}" + (f" | motivo: {extra_reason}" if extra_reason else ""))

                has_text_criteria = address_active or condo_active
                final_match = (address_match or condo_match) if has_text_criteria else True
                if not final_match:
                    log_fn("[imovel] Sem match final (address OU condominio).")
                    continue

                created_date, created_fragment, created_reason = extract_property_created_date(detail_page)
                if created_date:
                    log_fn(
                        f"[imovel] Data de criacao extraida: {created_fragment!r} -> {created_date.isoformat()}"
                    )
                else:
                    log_fn(f"[imovel] Data de criacao indisponivel: {created_reason}")

                if recent_days_applied:
                    assert window_start is not None and window_end is not None
                    if created_date is None:
                        log_fn("[imovel] Descartado por recent_days: data desconhecida.")
                        continue
                    if not (window_start <= created_date <= window_end):
                        log_fn(
                            "[imovel] Descartado por recent_days: "
                            f"{created_date.isoformat()} fora da janela {window_start.isoformat()}..{window_end.isoformat()}."
                        )
                        continue
                    log_fn("[imovel] Passou no filtro recent_days.")

                listing_id = extract_listing_id(property_url)
                match_key = listing_id or property_url
                if match_key not in match_keys:
                    match_keys.add(match_key)
                    matches.append(
                        {
                            "listing_id": listing_id,
                            "property_url": property_url,
                            "address_extracted": extracted_address,
                            "address_match": address_match,
                            "condominium_match": condo_match,
                        }
                    )
                log_fn(f"[match] Match final encontrado: {property_url} | matches={len(matches)}")
                blocked_streak = 0
            except BlockedPageError as exc:
                blocked_streak += 1
                total_blocked_details += 1
                log_fn(f"[erro] Falha no imovel {property_url}: {exc}")
                if blocked_streak >= MAX_CONSECUTIVE_BLOCKED_DETAILS:
                    log_fn(
                        "[anti-bot] Bloqueio em sequencia detectado. "
                        "Aplicando cooldown para tentar recuperar sessao."
                    )
                    recovered = False
                    if not headless and not detail_page.is_closed():
                        recovered = wait_for_manual_unblock(
                            detail_page,
                            log_fn=log_fn,
                            max_wait_seconds=MANUAL_UNBLOCK_WAIT_SECONDS * 2,
                        )
                    if not recovered:
                        pause_with_jitter(
                            listing_page,
                            BLOCK_COOLDOWN_MS,
                            log_fn=log_fn,
                            reason="cooldown por bloqueio consecutivo na pagina de detalhe",
                        )
                    blocked_streak = 0
                if total_blocked_details >= MAX_TOTAL_BLOCKED_DETAILS:
                    error_message = (
                        "Bloqueio anti-bot persistente. Limite de tentativas de recuperacao atingido."
                    )
                    blocked = True
                    stop_reason = "blocked_or_failed"
                    log_fn(f"[fim] {error_message}")
                    abort_run = True
                    break
            except Exception as exc:  # noqa: BLE001
                log_fn(f"[erro] Falha no imovel {property_url}: {exc}")
                if is_closed_target_error(exc):
                    error_message = "Navegador/aba fechado durante a execucao."
                    stop_reason = "blocked_or_failed"
                    log_fn(f"[fim] {error_message}")
                    abort_run = True
                    break
                blocked_streak = 0

        if error_message or abort_run:
            if not stop_reason:
                stop_reason = "blocked_or_failed"
            break

        scanned_pages += 1
        last_page_processed = current_page_number
        if scanned_pages >= FIXED_MAX_PAGES:
            print(
                f"[checkpoint] limite fixo atingido apos processar pagina: "
                f"scanned_pages={scanned_pages} fixed_max_pages={FIXED_MAX_PAGES}",
                flush=True,
            )
            log_fn(
                f"[checkpoint] limite fixo atingido apos processar pagina: "
                f"scanned_pages={scanned_pages} fixed_max_pages={FIXED_MAX_PAGES}"
            )
            log_fn(f"[fim] Limite fixo de paginas={FIXED_MAX_PAGES} atingido.")
            stop_reason = "fixed_limit_reached"
            break

        pause_with_jitter(
            listing_page,
            PAGE_CHANGE_DELAY_MS,
            log_fn=log_fn,
            reason="preparacao para proxima pagina",
        )
        has_next, next_selector = goto_next_page(
            listing_page,
            timeout=timeout,
            log_fn=log_fn,
            max_page_hint=max_page_hint,
        )
        if not has_next:
            blocked_listing, blocked_reason = is_probably_blocked_page(listing_page)
            if blocked_listing:
                error_message = f"Bloqueio anti-bot na paginacao. {blocked_reason}."
                stop_reason = "blocked_or_failed"
                log_fn(f"[fim] {error_message}")
            else:
                if max_page_hint is not None and current_page_number >= max_page_hint:
                    stop_reason = "reached_last_page"
                    log_fn(
                        f"[fim] Ultima pagina atingida (pagina {current_page_number}/{max_page_hint})."
                    )
                else:
                    stop_reason = "no_next_page"
                    log_fn("[fim] Nao existe proxima pagina.")
            break
        card_selector = next_selector
        current_page_number = max(1, parse_page_number(listing_page.url))

    if detail_page and not detail_page.is_closed():
        detail_page.close()

    has_more_pages = False
    if stop_reason == "fixed_limit_reached":
        if max_page_hint is not None:
            has_more_pages = last_page_processed < max_page_hint
            if has_more_pages:
                log_fn(
                    "[batch] limite fixo atingido; ha mais paginas segundo max_page_hint, "
                    "continuar no proximo lote."
                )
            else:
                log_fn(
                    "[batch] limite fixo atingido, mas max_page_hint indica fim; "
                    "encerrando."
                )
        else:
            has_more_pages = True
            log_fn(
                "[batch] limite fixo atingido; sem max_page_hint, "
                "assumindo que ha mais paginas e continuando no proximo lote."
            )
    elif stop_reason in {"no_next_page", "reached_last_page", "blocked_or_failed"}:
        has_more_pages = False
    else:
        if max_page_hint is not None:
            has_more_pages = last_page_processed < max_page_hint
        else:
            if not listing_page.is_closed():
                has_more_pages = detect_has_next_page_candidate(listing_page, max_page_hint)

    next_start_page = last_page_processed + 1 if has_more_pages else None

    if not stop_reason:
        if scanned_pages >= FIXED_MAX_PAGES:
            stop_reason = "fixed_limit_reached"
        elif error_message:
            stop_reason = "blocked_or_failed"
        else:
            stop_reason = "no_next_page"

    if max_page_hint is not None:
        log_fn(f"[batch] max_page_hint={max_page_hint} (total previsto)")
    log_fn(f"[batch] last_page_processed={last_page_processed}")
    log_fn(f"[batch] next_start_page={next_start_page}")
    if has_more_pages and next_start_page is not None:
        log_fn(f"[batch] proxima pagina sugerida={next_start_page}")
    else:
        log_fn("[batch] sem proxima pagina; encerrando fluxo.")
    log_fn(f"[batch] has_more_pages={str(has_more_pages).lower()}")
    log_fn(f"[batch] stop_reason={stop_reason}")

    context.close()
    total_matches = len(matches)
    status = "error" if error_message else ("encontrado" if total_matches else "nao_encontrado")
    return SearchResult(
        status=status,
        start_page=start_page,
        fixed_max_pages=FIXED_MAX_PAGES,
        pages_processed=scanned_pages,
        last_page_processed=last_page_processed,
        next_start_page=next_start_page,
        max_page_hint=max_page_hint,
        has_more_pages=has_more_pages,
        stop_reason=stop_reason,
        visited_properties=visited_count,
        elapsed_seconds=0.0,
        total_matches=total_matches,
        matches=matches,
        error_message=error_message,
    )


def run_search(
    listing_url: str,
    target_address: Optional[str],
    condominium: Optional[str],
    headless: bool,
    timeout: int,
    start_page: int = 1,
    recent_days: Optional[int] = None,
    log_fn: Optional[LogFn] = None,
) -> SearchResult:
    logs: list[str] = []

    def logger(message: str) -> None:
        logs.append(message)
        if log_fn:
            log_fn(message)
        else:
            default_log(message)

    normalized_address = normalize_for_contains((target_address or "").strip())
    normalized_condo = normalize_spaces_only((condominium or "").strip())
    recent_days_applied = recent_days is not None
    if start_page < 1:
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=0,
            last_page_processed=0,
            next_start_page=None,
            max_page_hint=None,
            has_more_pages=False,
            stop_reason="invalid_input",
            visited_properties=0,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            logs=logs,
            error_message="start_page deve ser >= 1.",
        )
    if not normalized_address and not normalized_condo and not recent_days_applied:
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=0,
            last_page_processed=start_page - 1,
            next_start_page=None,
            max_page_hint=None,
            has_more_pages=False,
            stop_reason="invalid_input",
            visited_properties=0,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            logs=logs,
            error_message="Informe ao menos um criterio: endereco (parcial), condominio ou dias recentes.",
        )

    try:
        recent_days = validate_recent_days(recent_days)
    except ValueError as exc:
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=0,
            last_page_processed=start_page - 1,
            next_start_page=None,
            max_page_hint=None,
            has_more_pages=False,
            stop_reason="invalid_input",
            visited_properties=0,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            logs=logs,
            error_message=str(exc),
        )

    started_at = time.perf_counter()
    try:
        with sync_playwright() as playwright:
            browser = launch_browser(playwright, headless=headless)
            try:
                print(
                    "[checkpoint] run_search start_page="
                    f"{start_page} fixed_max_pages={FIXED_MAX_PAGES}",
                    flush=True,
                )
                logger(
                    "[checkpoint] run_search start_page="
                    f"{start_page} fixed_max_pages={FIXED_MAX_PAGES}"
                )
                logger(
                    f"[limite] Execucao limitada a {FIXED_MAX_PAGES} paginas para reduzir bloqueio do Zap."
                )
                result = find_property_url(
                    browser=browser,
                    listing_url=listing_url,
                    target_address=target_address,
                    condominium=condominium,
                    headless=headless,
                    timeout=timeout,
                    start_page=start_page,
                    recent_days=recent_days,
                    log_fn=logger,
                )
            finally:
                browser.close()
    except Exception as exc:  # noqa: BLE001
        logger(f"[fatal] Erro inesperado: {exc}")
        return SearchResult(
            status="error",
            start_page=start_page,
            fixed_max_pages=FIXED_MAX_PAGES,
            pages_processed=0,
            last_page_processed=start_page - 1,
            next_start_page=None,
            max_page_hint=None,
            has_more_pages=False,
            stop_reason="blocked_or_failed",
            visited_properties=0,
            elapsed_seconds=0.0,
            total_matches=0,
            matches=[],
            logs=logs,
            error_message=str(exc),
        )

    elapsed = time.perf_counter() - started_at
    result.elapsed_seconds = round(elapsed, 2)
    result.logs = logs
    logger(
        f"[stats] Tempo total: {result.elapsed_seconds:.2f}s | paginas={result.pages_processed} "
        f"| visitados={result.visited_properties}"
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Encontra URLs de imoveis no Zap por endereco parcial e/ou condominio."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="URL da listagem do Zap.")
    parser.add_argument("--address", default=DEFAULT_ADDRESS, help="Endereco parcial para match por contains.")
    parser.add_argument("--condominium", default="", help="Nome do condominio para busca por contains na descricao.")
    parser.add_argument("--headless", default="true", type=parse_bool, help="true/false.")
    parser.add_argument("--start-page", type=int, default=1, help="Pagina inicial (>= 1).")
    parser.add_argument("--recent-days", type=int, default=None, help="Filtra anuncios criados nos ultimos N dias (0..30).")
    parser.add_argument("--timeout", type=int, default=30000, help="Timeout em ms.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_search(
        listing_url=args.url,
        target_address=args.address,
        condominium=args.condominium,
        headless=args.headless,
        timeout=args.timeout,
        start_page=args.start_page,
        recent_days=args.recent_days,
    )

    if result.url:
        for matched_url in result.property_urls:
            print(matched_url, flush=True)

    print(json.dumps(result.cli_payload(), ensure_ascii=False))
    return 0 if result.status == "encontrado" else (1 if result.status == "error" else 0)


if __name__ == "__main__":
    sys.exit(main())
