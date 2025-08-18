# main.py — watchdog + keepalive + retry + filtro per chat_id/username robusto
import os
import re
import asyncio
import time
import logging
import string
from typing import Optional, List, Tuple, Any, Dict, Set
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import aiohttp
from aiohttp import ClientTimeout, web
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaWebPage

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# ---------------- CONFIG (env) ----------------
API_ID = int(os.environ['API_ID'])
API_HASH = os.environ['API_HASH']
STRING_SESSION = os.environ['STRING_SESSION']

DEST_CHANNEL_RAW = os.environ.get('DEST_CHANNEL', '@PixelAffari')
AFFILIATE_TAG = os.environ.get('AFFILIATE_TAG', 'pixelofferte-21')

FORWARD_ALL = os.environ.get('FORWARD_ALL', 'true').lower() in ('1', 'true', 'yes')
ANTI_SPAM_DELAY = float(os.environ.get('ANTI_SPAM_DELAY', '5'))
SEEN_TTL = int(os.environ.get('SEEN_TTL', str(24 * 3600)))
PORT = int(os.environ.get('PORT', '8080'))
REQUEST_TIMEOUT = float(os.environ.get('REQUEST_TIMEOUT', '8'))

# Nuovi parametri
KEEPALIVE_INTERVAL = int(os.environ.get('KEEPALIVE_INTERVAL', '300'))  # sec
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
RETRY_BASE_DELAY = float(os.environ.get('RETRY_BASE_DELAY', '2'))

try:
    DEST_CHANNEL = int(DEST_CHANNEL_RAW)
except Exception:
    DEST_CHANNEL = DEST_CHANNEL_RAW

# ---------------- regex e utilità ----------------
LINK_RE = re.compile(r'https?://[^\s\)\]\>"]+')

def extract_asin_from_amazon_path(path: str) -> Optional[str]:
    if not path:
        return None
    for pattern in (
        r'/dp/([A-Z0-9]{10})',
        r'/gp/product/([A-Z0-9]{10})',
        r'/gp/aw/d/([A-Z0-9]{10})',
        r'/product/([A-Z0-9]{10})'
    ):
        m = re.search(pattern, path)
        if m:
            return m.group(1)
    return None

def build_affiliate_amazon_url(original_url: str, tag: str) -> str:
    """Forza/aggiunge il parametro tag e, se c’è ASIN, normalizza in /dp/ASIN/."""
    try:
        p = urlparse(original_url)
        host = (p.netloc or "").lower()
        if 'amazon.' not in host:
            return original_url

        qs = parse_qs(p.query)
        qs['tag'] = [tag]

        asin = extract_asin_from_amazon_path(p.path)
        if asin:
            new_path = f'/dp/{asin}/'
            new_query = urlencode({k: v[0] for k, v in qs.items()})
            return urlunparse((p.scheme, p.netloc, new_path, '', new_query, ''))

        new_query = urlencode({k: v[0] for k, v in qs.items()})
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return original_url

def _norm_uname(u: str) -> str:
    return (u or "").lower().lstrip('@').strip()

# ---------------- carica canali ----------------
def load_sources(fname: str = 'canali.txt') -> List[str]:
    if not os.path.exists(fname):
        logging.warning(f"{fname} non trovato. Crealo e inserisci username canali, uno per riga.")
        return []
    with open(fname, 'r', encoding='utf-8') as f:
        return [ln.strip() for ln in f if ln.strip()]

SOURCE_CHANNELS: List[str] = load_sources()
SOURCE_UNAMES: Set[str] = {_norm_uname(u) for u in SOURCE_CHANNELS}
logging.info(f"Canali sorgente caricati: {SOURCE_CHANNELS}")

# ---------------- dedupe cache ----------------
SEEN: Dict[Tuple[int, int], float] = {}  # (chat_id, msg_id) -> timestamp

async def cleanup_seen_task() -> None:
    while True:
        now = time.time()
        to_del = [k for k, v in list(SEEN.items()) if now - v > SEEN_TTL]
        for k in to_del:
            del SEEN[k]
        await asyncio.sleep(600)

# ---------------- globals ----------------
client: Optional[TelegramClient] = None
HTTP_SESSION: Optional[aiohttp.ClientSession] = None

# Mappa di supporto: username -> chat_id e set di chat_id ammessi
SOURCE_ID_BY_UNAME: Dict[str, int] = {}
SOURCE_IDS: Set[int] = set()

# ---------------- network helpers ----------------
async def resolve_redirect(url: str) -> str:
    global HTTP_SESSION
    if not HTTP_SESSION:
        return url
    try:
        timeout = ClientTimeout(total=REQUEST_TIMEOUT)
        async with HTTP_SESSION.get(url, allow_redirects=True, timeout=timeout) as resp:
            return str(resp.url)
    except Exception as e:
        logging.debug(f"Impossibile risolvere redirect per {url}: {e}")
        return url

async def transform_links_in_text_async(text: str) -> Tuple[str, List[Tuple[str, str]]]:
    if not text:
        return text, []
    links = LINK_RE.findall(text)
    replaced = text
    fixed_links: List[Tuple[str, str]] = []

    for orig in links:
        stripped = orig.rstrip(string.punctuation)
        suffix = orig[len(stripped):]
        candidate = stripped

        parsed = urlparse(stripped)
        host = (parsed.netloc or "").lower()

        # shorteners amzn.*
        if 'amzn.to' in host or (host.startswith('amzn.') and 'amazon.' not in host):
            candidate = await resolve_redirect(stripped)

        final_host = (urlparse(candidate).netloc or "").lower()
        if 'amazon.' in final_host:
            new_url = build_affiliate_amazon_url(candidate, AFFILIATE_TAG)
            if new_url and new_url != candidate:
                replacement = new_url + suffix
                replaced = replaced.replace(orig, replacement, 1)
                fixed_links.append((orig, replacement))
                logging.info(f"[AmazonTagFix] {orig} -> {replacement}")
            else:
                fallback = build_affiliate_amazon_url(stripped, AFFILIATE_TAG)
                if fallback != stripped:
                    replacement = fallback + suffix
                    replaced = replaced.replace(orig, replacement, 1)
                    fixed_links.append((orig, replacement))
                    logging.info(f"[AmazonTagFix-fallback] {orig} -> {replacement}")
        else:
            if 'amazon.' in host:
                new_url = build_affiliate_amazon_url(stripped, AFFILIATE_TAG)
                if new_url != stripped:
                    replacement = new_url + suffix
                    replaced = replaced.replace(orig, replacement, 1)
                    fixed_links.append((orig, replacement))
                    logging.info(f"[AmazonTagFix-direct] {orig} -> {replacement}")

    return replaced, fixed_links

async def rebuild_buttons_async(original_buttons) -> Optional[List[List[Button]]]:
    if not original_buttons:
        return None
    new = []
    try:
        for row in original_buttons:
            new_row = []
            for b in row:
                text = getattr(b, 'text', None)
                url = getattr(b, 'url', None)
                if not text and getattr(b, 'button', None):
                    text = getattr(b.button, 'text', None)
                    url = getattr(b.button, 'url', None)
                if url and text:
                    parsed = urlparse(url)
                    host = (parsed.netloc or "").lower()
                    final_url = url
                    if 'amzn.to' in host or (host.startswith('amzn.') and 'amazon.' not in host):
                        final_url = await resolve_redirect(url)
                    if 'amazon.' in (urlparse(final_url).netloc or "").lower():
                        final_url = build_affiliate_amazon_url(final_url, AFFILIATE_TAG)
                    new_row.append(Button.url(text, final_url))
                else:
                    try:
                        new_row.append(b)
                    except Exception:
                        logging.debug("Bottone non URL non mantenuto")
            new.append(new_row)
        return new
    except Exception:
        logging.exception("Errore durante rebuild_buttons_async")
        return None

# ---------------- send con retry ----------------
async def send_message_with_retry(dest: Any, text: str, *, buttons=None, link_preview: bool = True,
                                  max_retries: int = MAX_RETRIES, base_delay: float = RETRY_BASE_DELAY) -> None:
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        try:
            await client.send_message(dest, text, buttons=buttons, link_preview=link_preview)
            return
        except Exception as e:
            logging.error(f"send_message tentativo {attempt}/{max_retries} fallito: {e}")
            if attempt == max_retries:
                raise
            await asyncio.sleep(delay)
            delay *= 2

async def send_file_with_retry(dest: Any, media: Any, *, caption: str, buttons=None,
                               max_retries: int = MAX_RETRIES, base_delay: float = RETRY_BASE_DELAY) -> None:
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        try:
            await client.send_file(dest, media, caption=caption, buttons=buttons)
            return
        except Exception as e:
            logging.error(f"send_file tentativo {attempt}/{max_retries} fallito: {e}")
            if attempt == max_retries:
                raise
            await asyncio.sleep(delay)
            delay *= 2

# ---------------- keepalive ----------------
async def keep_channels_alive():
    while True:
        if client and (SOURCE_IDS or SOURCE_CHANNELS):
            targets = list(SOURCE_IDS)  # prefer chat_id se li abbiamo
            if not targets:
                targets = SOURCE_CHANNELS
            for ch in targets:
                try:
                    await client.get_messages(ch, limit=1)
                    logging.debug(f"[KeepAlive] ping su {ch}")
                except Exception as e:
                    logging.warning(f"[KeepAlive] errore su {ch}: {e}")
        await asyncio.sleep(KEEPALIVE_INTERVAL)

# ---------------- handler ----------------
async def handler(event):
    try:
        # ricava username (se presente) e chat_id
        username = _norm_uname(getattr(getattr(event, 'chat', None), 'username', '') or '')
        cid = getattr(event, 'chat_id', None)

        allowed = False
        if cid is not None and cid in SOURCE_IDS:
            allowed = True
        elif username and username in SOURCE_UNAMES:
            allowed = True

        if not allowed:
            # Loggare una volta ogni tanto aiuta a diagnosticare, ma non intasiamo i log
            logging.debug(f"Skip msg da chat_id={cid} uname=@{username} (non in lista)")
            return

        msg_id = event.message.id
        key = (cid or 0, msg_id)
        if key in SEEN:
            logging.info("⏩ Ignoro messaggio duplicato")
            return
        SEEN[key] = time.time()

        raw_text = event.message.message or ""
        new_text, fixed_links = await transform_links_in_text_async(raw_text)

        if not FORWARD_ALL:
            found = any(('amazon.' in l or 'zalando' in l or 'ebay.' in l) for l in LINK_RE.findall(raw_text or ""))
            if not found:
                logging.info("🔍 Messaggio filtrato (no link target)")
                return

        buttons = None
        if getattr(event.message, 'buttons', None):
            buttons = await rebuild_buttons_async(event.message.buttons)

        # WebPage → come testo
        if event.message.media and isinstance(event.message.media, MessageMediaWebPage):
            webpage = getattr(event.message.media, 'webpage', None)
            extracted_url = getattr(webpage, 'url', None) if webpage is not None else None
            out_text = new_text or " "
            if extracted_url and 'amazon.' in extracted_url:
                out_text = out_text.replace(extracted_url, build_affiliate_amazon_url(extracted_url, AFFILIATE_TAG))
            try:
                await send_message_with_retry(DEST_CHANNEL, out_text, buttons=buttons, link_preview=True)
                logging.info(f"✅ Inviato WebPage-as-text (msg {msg_id}) da @{username or cid}")
            except Exception:
                logging.exception("Errore invio WebPage-as-text")

        # altri media
        elif event.message.media:
            caption = new_text or " "
            try:
                await send_file_with_retry(DEST_CHANNEL, event.message.media, caption=caption, buttons=buttons)
                logging.info(f"✅ Inviato media (msg {msg_id}) da @{username or cid}")
            except Exception as e:
                logging.warning(f"Forward media fallito ({e}), invio fallback testo.")
                try:
                    out_text = caption or " "
                    await send_message_with_retry(DEST_CHANNEL, out_text, buttons=buttons, link_preview=True)
                    logging.info("✅ Inviato testo fallback per media")
                except Exception:
                    logging.exception("Errore invio fallback per media")

        # solo testo
        else:
            out_text = new_text or " "
            try:
                await send_message_with_retry(DEST_CHANNEL, out_text, buttons=buttons, link_preview=True)
                logging.info(f"✅ Inviato testo (msg {msg_id}) da @{username or cid}")
            except Exception:
                logging.exception("Errore invio testo")

        for old, new in fixed_links:
            logging.info(f"🔁 Link modificato: {old} -> {new}")

        await asyncio.sleep(ANTI_SPAM_DELAY)

    except Exception:
        logging.exception("Errore nel handler")

# ---------------- health ----------------
async def handle_ping(request):
    logging.info("Ping ricevuto (UptimeRobot o browser).")
    return web.Response(text="PixelAffari alive")

async def start_health_server() -> None:
    app = web.Application()
    app.router.add_get('/', handle_ping)
    app.router.add_get('/health', handle_ping)
    app.router.add_head('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logging.info(f"Health server avviato su porta {PORT} (per keepalive).")

# ---------------- monitor & supervisor ----------------
async def monitor_connection(check_interval: int = 60, max_failures: int = 3) -> None:
    fails = 0
    while True:
        await asyncio.sleep(check_interval)
        try:
            if client is None:
                fails += 1
                logging.warning("Monitor: client è None")
            else:
                await asyncio.wait_for(client.get_me(), timeout=10)
                fails = 0
        except Exception as e:
            fails += 1
            logging.warning(f"Monitor: check fallito ({fails}/{max_failures}): {e}")
        if fails >= max_failures:
            logging.error("Monitor: connessione instabile. Richiedo restart del client.")
            return

async def prime_entities() -> None:
    """Risolvi entity e costruisci mapping username->id e set ID per match robusto."""
    global SOURCE_ID_BY_UNAME, SOURCE_IDS
    SOURCE_ID_BY_UNAME = {}
    SOURCE_IDS = set()

    if not SOURCE_CHANNELS:
        return

    for ch in SOURCE_CHANNELS:
        uname = _norm_uname(ch)
        try:
            ent = await client.get_entity(ch)
            cid = getattr(ent, 'id', None)
            if isinstance(cid, int):
                SOURCE_ID_BY_UNAME[uname] = cid
                SOURCE_IDS.add(cid)
                logging.info(f"Prime entity ok: {ch} -> chat_id={cid}")
            else:
                logging.warning(f"Prime entity: id non valido per {ch}")
        except Exception as e:
            logging.warning(f"Prime entity fallita per {ch}: {e}")

async def run_bot_once(backoff_after: int = 0) -> None:
    global client, HTTP_SESSION
    if backoff_after:
        logging.info(f"Attendo {backoff_after}s prima di riavviare...")
        await asyncio.sleep(backoff_after)

    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    HTTP_SESSION = aiohttp.ClientSession(timeout=ClientTimeout(total=REQUEST_TIMEOUT))

    await client.start()

    # Handler globale (niente filtro chats: filtriamo dentro l'handler per id/username)
    client.add_event_handler(handler, events.NewMessage(incoming=True))
    logging.info("Handler globale registrato (filtro per id/username interno).")

    # Warm-up entity cache e popolamento ID
    await prime_entities()

    # Task di servizio
    health_task = asyncio.create_task(start_health_server())
    monitor_task = asyncio.create_task(monitor_connection())
    keepalive_task = asyncio.create_task(keep_channels_alive())
    cleanup_task = asyncio.create_task(cleanup_seen_task())

    logging.info("🚀 PixelAffari userbot avviato e in ascolto...")

    try:
        await monitor_task
    finally:
        try:
            client.remove_event_handler(handler)
        except Exception:
            pass
        try:
            if HTTP_SESSION and not HTTP_SESSION.closed:
                await HTTP_SESSION.close()
        except Exception:
            pass
        try:
            await client.disconnect()
        except Exception:
            pass
        for task in (health_task, keepalive_task, cleanup_task):
            try:
                if not task.done():
                    task.cancel()
                    await asyncio.sleep(0)
            except Exception:
                pass
        logging.info("Run ended, risorse pulite. Torno al supervisor per possibile restart.")

async def supervisor_loop():
    backoff = 5
    while True:
        try:
            await run_bot_once(backoff_after=0)
            logging.info(f"Supervisor: riavvio in {backoff}s")
        except Exception:
            logging.exception("Supervisor: run_bot_once ha sollevato eccezione")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 300)

# ---------------- main ----------------
async def main() -> None:
    await supervisor_loop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Arresto richiesto manualmente.")






