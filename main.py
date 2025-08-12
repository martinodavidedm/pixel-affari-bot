# main.py — versione definitiva corretta
import os
import re
import asyncio
import time
import logging
import string
from typing import Optional, List, Tuple
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
AFFILIATE_TAG = os.environ.get('AFFILIATE_TAG', 'pixelofferte-21')  # inserisci solo il tag, es: pixelofferte-21
FORWARD_ALL = os.environ.get('FORWARD_ALL', 'true').lower() in ('1', 'true', 'yes')
ANTI_SPAM_DELAY = float(os.environ.get('ANTI_SPAM_DELAY', '5'))
SEEN_TTL = int(os.environ.get('SEEN_TTL', str(24 * 3600)))
PORT = int(os.environ.get('PORT', '8080'))
REQUEST_TIMEOUT = float(os.environ.get('REQUEST_TIMEOUT', '8'))

# Normalizza DEST_CHANNEL (accetta ID numerico o @username)
try:
    DEST_CHANNEL = int(DEST_CHANNEL_RAW)
except Exception:
    DEST_CHANNEL = DEST_CHANNEL_RAW

# ---------------- regex e utilità ----------------
LINK_RE = re.compile(r'https?://[^\s\)\]\>"]+')

def extract_asin_from_amazon_path(path: str) -> Optional[str]:
    if not path:
        return None
    for pattern in (r'/dp/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})', r'/gp/aw/d/([A-Z0-9]{10})', r'/product/([A-Z0-9]{10})'):
        m = re.search(pattern, path)
        if m:
            return m.group(1)
    return None

def build_affiliate_amazon_url(original_url: str, tag: str) -> str:
    """
    Sempre sovrascrive/aggiunge il parametro tag con il tag fornito.
    Se trova ASIN, ricostruisce l'URL come /dp/ASIN/ mantenendo altri parametri se presenti.
    """
    try:
        p = urlparse(original_url)
        host = (p.netloc or "").lower()
        if 'amazon.' not in host:
            return original_url

        qs = parse_qs(p.query)
        # sovrascrivi il tag
        q = {k: v for k, v in qs.items()}
        q['tag'] = [tag]

        asin = extract_asin_from_amazon_path(p.path)
        if asin:
            new_path = f'/dp/{asin}/'
            new_query = urlencode({k: v[0] for k, v in q.items()})
            new_url = urlunparse((p.scheme, p.netloc, new_path, '', new_query, ''))
            return new_url

        new_query = urlencode({k: v[0] for k, v in q.items()})
        new_url = urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
        return new_url
    except Exception:
        return original_url

# ---------------- carica canali ----------------
def load_sources(fname: str = 'canali.txt') -> List[str]:
    if not os.path.exists(fname):
        logging.warning(f"{fname} non trovato. Crealo e inserisci username canali, uno per riga.")
        return []
    with open(fname, 'r', encoding='utf-8') as f:
        return [ln.strip() for ln in f if ln.strip()]

SOURCE_CHANNELS = load_sources()
logging.info(f"Canali sorgente caricati: {SOURCE_CHANNELS}")

# ---------------- dedupe cache ----------------
SEEN = {}  # (chat_id, msg_id) -> timestamp

async def cleanup_seen_task() -> None:
    while True:
        now = time.time()
        to_del = [k for k, v in list(SEEN.items()) if now - v > SEEN_TTL]
        for k in to_del:
            del SEEN[k]
        await asyncio.sleep(600)

# ---------------- Telethon client ----------------
client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# HTTP session per risolvere short-links (amzn.to)
HTTP_SESSION: Optional[aiohttp.ClientSession] = None

async def resolve_redirect(url: str) -> str:
    """
    Segue redirect (es. amzn.to) e ritorna URL finale.
    Se fallisce, ritorna l'URL originale.
    """
    global HTTP_SESSION
    if not HTTP_SESSION:
        return url
    try:
        timeout = ClientTimeout(total=REQUEST_TIMEOUT)
        async with HTTP_SESSION.get(url, allow_redirects=True, timeout=timeout) as resp:
            final = str(resp.url)
            logging.debug(f"Resolved {url} -> {final} (status {resp.status})")
            return final
    except Exception as e:
        logging.debug(f"Impossibile risolvere redirect per {url}: {e}")
        return url

async def transform_links_in_text_async(text: str) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Risolve short links (amzn.to, amzn.*) e sovrascrive/aggiunge il tag di affiliazione per amazon.*.
    Restituisce (testo_sostituito, lista_coppie_old_new).
    """
    if not text:
        return text, []
    links = LINK_RE.findall(text)
    replaced = text
    fixed_links: List[Tuple[str, str]] = []

    for orig in links:
        # conserva eventuale punteggiatura finale
        stripped = orig.rstrip(string.punctuation)
        suffix = orig[len(stripped):]
        candidate = stripped

        parsed = urlparse(stripped)
        host = (parsed.netloc or "").lower()

        # se short-link amzn.to o amzn.* (non amazon), prova a risolvere
        if 'amzn.to' in host or (host.startswith('amzn.') and 'amazon.' not in host):
            resolved = await resolve_redirect(stripped)
            candidate = resolved

        final_host = (urlparse(candidate).netloc or "").lower()
        if 'amazon.' in final_host:
            new_url = build_affiliate_amazon_url(candidate, AFFILIATE_TAG)
            if new_url and new_url != candidate:
                replacement = new_url + suffix
                replaced = replaced.replace(orig, replacement, 1)
                fixed_links.append((orig, replacement))
                logging.info(f"[AmazonTagFix] {orig} -> {replacement}")
            else:
                # fallback: prova con stripped
                fallback = build_affiliate_amazon_url(stripped, AFFILIATE_TAG)
                if fallback != stripped:
                    replacement = fallback + suffix
                    replaced = replaced.replace(orig, replacement, 1)
                    fixed_links.append((orig, replacement))
                    logging.info(f"[AmazonTagFix-fallback] {orig} -> {replacement}")
        else:
            # host originario contenente amazon.* (ma matching differente)
            if 'amazon.' in host:
                new_url = build_affiliate_amazon_url(stripped, AFFILIATE_TAG)
                if new_url != stripped:
                    replacement = new_url + suffix
                    replaced = replaced.replace(orig, replacement, 1)
                    fixed_links.append((orig, replacement))
                    logging.info(f"[AmazonTagFix-direct] {orig} -> {replacement}")

    return replaced, fixed_links

async def rebuild_buttons_async(original_buttons) -> Optional[List[List[Button]]]:
    """
    Ricostruisce i bottoni URL aggiornando gli href amazon/amzn.
    Ritorna lista di liste di Button o None.
    """
    if not original_buttons:
        return None
    new = []
    try:
        for row in original_buttons:
            new_row = []
            for b in row:
                # prova a leggere text e url
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
                    # conserva oggetto non-URL se possibile
                    try:
                        new_row.append(b)
                    except Exception:
                        logging.debug("Bottone non URL non mantenuto")
            new.append(new_row)
        return new
    except Exception:
        logging.exception("Errore durante rebuild_buttons_async")
        return None

# ---------- handler ----------
@events.register  # decoratore non usato direttamente, registro dinamicamente in main()
async def handler(event):
    try:
        chat_id = getattr(event.chat, 'id', None) or event.chat_id
        msg_id = event.message.id
        key = (chat_id, msg_id)
        if key in SEEN:
            logging.info("⏩ Ignoro messaggio duplicato")
            return
        SEEN[key] = time.time()

        raw_text = event.message.message or ""
        new_text, fixed_links = await transform_links_in_text_async(raw_text)

        # filtro se necessario
        if (not FORWARD_ALL):
            found = any(('amazon.' in l or 'zalando' in l or 'ebay.' in l) for l in LINK_RE.findall(raw_text or ""))
            if not found:
                logging.info("🔍 Messaggio filtrato (no link target)")
                return

        # ricostruisci bottoni se presenti (async)
        buttons = None
        if getattr(event.message, 'buttons', None):
            buttons = await rebuild_buttons_async(event.message.buttons)

        # MessageMediaWebPage -> invia come testo (non usare send_file)
        if event.message.media and isinstance(event.message.media, MessageMediaWebPage):
            webpage = getattr(event.message.media, 'webpage', None)
            extracted_url = getattr(webpage, 'url', None) if webpage is not None else None
            out_text = new_text or ""
            if extracted_url and 'amazon.' in extracted_url:
                out_text = out_text.replace(extracted_url, build_affiliate_amazon_url(extracted_url, AFFILIATE_TAG))
            if buttons:
                await client.send_message(DEST_CHANNEL, out_text or " ", buttons=buttons)
            else:
                await client.send_message(DEST_CHANNEL, out_text or " ")
            logging.info(f"✅ Inviato WebPage-as-text (msg {msg_id})")

        # Altri media (foto, video, doc) -> send_file (caption sempre str)
        elif event.message.media:
            caption: str = new_text or ""
            try:
                await client.send_file(DEST_CHANNEL, event.message.media, caption=caption, buttons=buttons)
                logging.info(f"✅ Inviato media (msg {msg_id}) da {getattr(event.chat,'username',chat_id)}")
            except Exception as e:
                logging.warning(f"Forward media fallito ({e}), invio fallback testo.")
                try:
                    out_text = caption or " "
                    if buttons:
                        await client.send_message(DEST_CHANNEL, out_text, buttons=buttons)
                    else:
                        await client.send_message(DEST_CHANNEL, out_text)
                    logging.info("✅ Inviato testo fallback per media")
                except Exception:
                    logging.exception("Errore invio fallback per media")

        # Solo testo
        else:
            out_text: str = new_text or " "
            try:
                if buttons:
                    await client.send_message(DEST_CHANNEL, out_text, buttons=buttons)
                else:
                    await client.send_message(DEST_CHANNEL, out_text)
                logging.info(f"✅ Inviato testo (msg {msg_id}) da {getattr(event.chat,'username',chat_id)}")
            except Exception:
                logging.exception("Errore invio testo")

        # log eventuali sostituzioni
        for old, new in fixed_links:
            logging.info(f"🔁 Link modificato: {old} -> {new}")

        # anti-spam
        await asyncio.sleep(ANTI_SPAM_DELAY)

    except Exception:
        logging.exception("Errore nel handler")

# ---------------------- small health server ----------------------
async def handle_ping(request):
    logging.info("Ping ricevuto (UptimeRobot o browser).")
    return web.Response(text="PixelAffari alive")

async def start_health_server() -> None:
    app = web.Application()
    app.router.add_get('/', handle_ping)
    app.router.add_get('/health', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logging.info(f"Health server avviato su porta {PORT} (per keepalive).")

# ---------------- main ----------------
async def main() -> None:
    global HTTP_SESSION
    # start background tasks
    asyncio.create_task(cleanup_seen_task())

    # crea sessione HTTP per risolvere short-links
    HTTP_SESSION = aiohttp.ClientSession(timeout=ClientTimeout(total=REQUEST_TIMEOUT))

    # avvia health server
    asyncio.create_task(start_health_server())

    # avvia client
    await client.start()

    # registra handler (senza await) solo se ci sono canali
    if SOURCE_CHANNELS:
        client.add_event_handler(handler, events.NewMessage(chats=SOURCE_CHANNELS))
        logging.info("Handler registrato per i canali sorgente.")
    else:
        logging.warning("Nessun canale sorgente caricato; aggiungi usernames a canali.txt e riavvia per attivare l'ascolto.")

    logging.info("🚀 PixelAffari userbot avviato e in ascolto...")
    try:
        await asyncio.Event().wait()
    finally:
        # pulizie
        if HTTP_SESSION and not HTTP_SESSION.closed:
            await HTTP_SESSION.close()
        await client.disconnect()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Arresto richiesto manualmente.")
