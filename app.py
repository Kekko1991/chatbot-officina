import os
import json
import re
import hmac
import hashlib
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, render_template_string, redirect, session
import anthropic
import tempfile
import requests as req
import openai
from google.oauth2 import service_account
from googleapiclient.discovery import build
import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CREDENZIALI ---
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
META_ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', '')
META_PHONE_NUMBER_ID = os.environ.get('META_PHONE_NUMBER_ID', '')
META_VERIFY_TOKEN = os.environ.get('META_VERIFY_TOKEN', 'chatbot_officina_2024')
META_APP_SECRET = os.environ.get('META_APP_SECRET', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')

# --- CONFIG CONCESSIONARIO ---
NOME = os.environ.get('NOME_CONCESSIONARIO', 'AutoPlus')
INDIRIZZO = os.environ.get('INDIRIZZO', 'Via Roma 123, 80100 Napoli')
TELEFONO = os.environ.get('TELEFONO_OFFICINA', '+39 081 123 4567')
WHATSAPP_OFFICINA = os.environ.get('WHATSAPP_OFFICINA', '+393312782211')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'brocar')
TEMPLATE_NAME = os.environ.get('TEMPLATE_NAME', 'auto_pronta')
TEMPLATE_PROMEMORIA = os.environ.get('TEMPLATE_PROMEMORIA', 'promemoria_appuntamento')
TEMPLATE_NON_PRONTA = os.environ.get('TEMPLATE_NON_PRONTA', 'auto_non_pronta')

ORARIO_APERTURA = os.environ.get('ORARIO_APERTURA', '08:30')
ORARIO_CHIUSURA = os.environ.get('ORARIO_CHIUSURA', '17:00')
PAUSA_INIZIO = os.environ.get('PAUSA_PRANZO_INIZIO', '13:00')
PAUSA_FINE = os.environ.get('PAUSA_PRANZO_FINE', '14:00')
SABATO_CHIUSURA = os.environ.get('SABATO_CHIUSURA', '13:00')
DURATA_SLOT = int(os.environ.get('DURATA_SLOT_MINUTI', '60'))
MAX_APPUNTAMENTI_SETTIMANA = int(os.environ.get('MAX_APPUNTAMENTI_SETTIMANA', '3'))
TZ_ROME = ZoneInfo('Europe/Rome')

# --- CLIENTS ---
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# --- GOOGLE CALENDAR ---
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
GOOGLE_CALENDAR_ID = os.environ.get('GOOGLE_CALENDAR_ID', '')

def get_calendar_service():
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_CALENDAR_ID:
        return None
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/calendar']
    )
    return build('calendar', 'v3', credentials=creds)

def crea_evento_calendar(slot, nome_cliente, triage_data):
    service = get_calendar_service()
    if not service:
        logger.warning('Google Calendar non configurato')
        return None
    pri = triage_data.get('priority', '')
    categoria = CATEGORY_LABELS.get(triage_data.get('category', ''), triage_data.get('category', ''))
    auto = triage_data.get('auto_cliente', '')
    sommario = nome_cliente
    if auto:
        sommario += ' - ' + auto
    if categoria:
        sommario += ' (' + categoria + ')'
    descrizione = 'CONSEGNA AUTO (ricovero 24-72h)\n\n'
    descrizione += 'Cliente: ' + nome_cliente + '\n'
    if auto:
        descrizione += 'Veicolo: ' + auto + '\n'
    if pri:
        config = PRIORITY_CONFIG.get(pri, {})
        descrizione += 'Priorita\': ' + config.get('label', pri) + '\n'
    if categoria:
        descrizione += 'Categoria: ' + categoria + '\n'
    if triage_data.get('summary'):
        descrizione += 'Problema: ' + triage_data['summary'] + '\n'
    if triage_data.get('recommendation'):
        descrizione += 'Consiglio: ' + triage_data['recommendation'] + '\n'
    evento = {
        'summary': sommario,
        'description': descrizione,
        'start': {'dateTime': slot['datetime_start'], 'timeZone': 'Europe/Rome'},
        'end': {'dateTime': slot['datetime_end'], 'timeZone': 'Europe/Rome'},
        'reminders': {'useDefault': False, 'overrides': [
            {'method': 'popup', 'minutes': 60},
        ]},
    }
    try:
        result = service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=evento).execute()
        logger.info('Evento Calendar creato: ' + result.get('id', ''))
        return result.get('id')
    except Exception as e:
        logger.error('Errore Google Calendar: ' + str(e))
        return None

def cancella_evento_calendar(event_id):
    if not event_id:
        return
    service = get_calendar_service()
    if not service:
        return
    try:
        service.events().delete(calendarId=GOOGLE_CALENDAR_ID, eventId=event_id).execute()
        logger.info('Evento Calendar cancellato: ' + event_id)
    except Exception as e:
        logger.error('Errore cancellazione Calendar: ' + str(e))

META_API_URL = 'https://graph.facebook.com/v21.0/' + META_PHONE_NUMBER_ID + '/messages'
META_HEADERS = {
    'Authorization': 'Bearer ' + META_ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

# --- TRASCRIZIONE VOCALI ---
openai_client = openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

def download_media(media_id):
    """Scarica un media da Meta e ritorna i bytes e il content type."""
    try:
        media_resp = req.get(
            'https://graph.facebook.com/v21.0/' + media_id,
            headers={'Authorization': 'Bearer ' + META_ACCESS_TOKEN},
            timeout=10
        )
        media_info = media_resp.json()
        media_url = media_info.get('url')
        mime_type = media_info.get('mime_type', '')
        if not media_url:
            logger.error('Media: URL non trovato per ' + media_id)
            return None, None

        data_resp = req.get(
            media_url,
            headers={'Authorization': 'Bearer ' + META_ACCESS_TOKEN},
            timeout=30
        )
        if not data_resp.ok:
            logger.error('Media: download fallito - ' + str(data_resp.status_code))
            return None, None

        return data_resp.content, mime_type
    except Exception as e:
        logger.error('Errore download media: ' + str(e))
        return None, None


def transcribe_audio(media_id):
    """Scarica un vocale da Meta e lo trascrive con Whisper."""
    try:
        audio_data, _ = download_media(media_id)
        if not audio_data:
            return None

        with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
            tmp.write(audio_data)
            tmp_path = tmp.name

        try:
            with open(tmp_path, 'rb') as audio_file:
                transcript = openai_client.audio.transcriptions.create(
                    model='whisper-1',
                    file=audio_file,
                    language='it'
                )
            logger.info('Vocale trascritto: ' + transcript.text[:50] + '...')
            return transcript.text
        finally:
            os.unlink(tmp_path)

    except Exception as e:
        logger.error('Errore trascrizione vocale: ' + str(e))
        return None


import base64

def download_image_as_base64(media_id):
    """Scarica un'immagine da Meta e la ritorna come base64 con il media type."""
    image_data, mime_type = download_media(media_id)
    if not image_data:
        return None, None
    # Meta restituisce mime_type tipo "image/jpeg", "image/png" ecc.
    if not mime_type:
        mime_type = 'image/jpeg'
    # Claude accetta: image/jpeg, image/png, image/gif, image/webp
    media_type = mime_type.split(';')[0].strip()
    b64 = base64.b64encode(image_data).decode('utf-8')
    logger.info('Immagine scaricata: ' + media_type + ', ' + str(len(image_data)) + ' bytes')
    return b64, media_type

# --- LOCK PER UTENTE (evita risposte sovrapposte) ---
user_locks = {}
_locks_lock = threading.Lock()

def get_user_lock(phone):
    with _locks_lock:
        if phone not in user_locks:
            user_locks[phone] = threading.Lock()
        return user_locks[phone]

def send_whatsapp_message(to_number, text):
    payload = {
        'messaging_product': 'whatsapp',
        'to': to_number,
        'type': 'text',
        'text': {'body': text}
    }
    try:
        r = req.post(META_API_URL, headers=META_HEADERS, json=payload)
        if r.status_code != 200:
            logger.error('Meta errore: ' + str(r.status_code) + ' ' + r.text)
        return r.status_code == 200
    except Exception as e:
        logger.error('Invio errore: ' + str(e))
        return False

def send_template_message(to_number, template_name, params):
    """Invia un messaggio template WhatsApp (per messaggi proattivi oltre le 24h)."""
    payload = {
        'messaging_product': 'whatsapp',
        'to': to_number,
        'type': 'template',
        'template': {
            'name': template_name,
            'language': {'code': 'it'},
            'components': [
                {
                    'type': 'body',
                    'parameters': [{'type': 'text', 'text': p} for p in params]
                }
            ]
        }
    }
    try:
        r = req.post(META_API_URL, headers=META_HEADERS, json=payload)
        if r.status_code != 200:
            logger.error('Template errore: ' + str(r.status_code) + ' ' + r.text)
            return False, r.text
        return True, 'OK'
    except Exception as e:
        logger.error('Template invio errore: ' + str(e))
        return False, str(e)

# --- SYSTEM PROMPT ---
SYSTEM_PROMPT = '''Sei l'assistente WhatsApp dell'officina ''' + NOME + ''' (''' + INDIRIZZO + ''', tel. ''' + TELEFONO + ''').

COMPITO: capire il problema dell'auto in modo preciso e dettagliato, classificarlo e fissare un appuntamento.

STILE DI COMUNICAZIONE:
- Sii diretto, professionale e schematico. Niente giri di parole.
- NON essere apprensivo o eccessivamente empatico. Niente frasi come "capisco la sua preoccupazione", "non si preoccupi", "siamo qui per lei".
- Se il cliente e' in panico o fermo in strada, digli brevemente di mettersi in sicurezza e vai avanti con le domande.
- Rispondi in 1-2 frasi brevi. MAI piu' di 3 frasi.
- Fai UNA domanda alla volta.
- Dai del "Lei".
- NON ripetere quello che il cliente ha detto.
- NON salutare se il cliente ha gia' descritto un problema.
- NON suggerire MAI al cliente di chiamare l'officina. Tu gestisci TUTTO.

RACCOLTA DETTAGLI (IMPORTANTE):
- Fai domande specifiche e tecniche per capire bene il problema.
- Chiedi SEMPRE: da quanto tempo c'e' il problema? In che condizioni si presenta (a freddo, a caldo, in curva, in frenata, ecc.)?
- Se il cliente e' vago, insisti educatamente per avere dettagli: "Mi descriva meglio: succede sempre o solo a volte? A che velocita'?"
- Non accontentarti di descrizioni generiche come "fa un rumore strano". Chiedi che tipo di rumore, da dove viene, quando si sente.
- Raccogli abbastanza informazioni per dare all'officina un quadro chiaro del problema.

DOPO 2-4 SCAMBI (quando hai abbastanza dettagli), rispondi SOLO con questo JSON (niente altro testo):
{"triage_complete":true,"priority":"CRITICA|ALTA|MEDIA|BASSA","category":"motore|trasmissione|freni|sterzo|sospensioni|impianto_elettrico|climatizzazione|carrozzeria|pneumatici|luci|tergicristalli|batteria|scarico|altro","summary":"Descrizione dettagliata del problema con tutti i dettagli raccolti","recommendation":"Cosa consigliamo","emotional_note":"calmo|preoccupato|ansioso|in_panico","preferred_datetime":"SOLO se il cliente ha indicato una data/ora preferita, scrivi qui in formato YYYY-MM-DD HH:MM. Altrimenti null."}

RICOVERO AUTO:
- L'officina accetta auto SOLO dal lunedi' al mercoledi' (consegna).
- L'auto resta in officina per un ricovero di 24-72 ore.
- Massimo 3 appuntamenti a settimana.
- Spiega al cliente che l'auto sara' in ricovero e che verra' contattato quando e' pronta.

PRIORITA':
- CRITICA: veicolo non guidabile, sicurezza compromessa
- ALTA: problema serio ma utilizzabile con cautela
- MEDIA: da risolvere ma non urgente
- BASSA: manutenzione ordinaria o estetica

FOTO:
- Se il problema e' VISIVO (spia accesa, graffio, ammaccatura, danno carrozzeria, pneumatico danneggiato, pezzo rotto visibile, perdita liquido), chiedi al cliente di mandare una foto. Dì qualcosa come: "Se puo', mi mandi una foto cosi' posso capire meglio la situazione."
- Se il problema e' INTERNO/MECCANICO (rumore, vibrazione, problema avviamento, freni che tirano, cambio duro, ecc.), NON chiedere foto perche' non servirebbero.
- La foto NON e' mai obbligatoria. Se il cliente non la manda, prosegui normalmente.
- Se il cliente manda una foto, analizzala attentamente e usala per migliorare la tua valutazione.

NON classificare al primo messaggio. Fai ALMENO 2 domande prima di classificare, per avere un quadro dettagliato del problema.'''

# --- SLOT E PRIORITA' ---
PRIORITY_CONFIG = {
    'CRITICA': {'emoji': '\U0001f534', 'label': 'URGENTE', 'min_days': 0, 'max_days': 7, 'slots': 3},
    'ALTA':    {'emoji': '\U0001f7e0', 'label': 'PRIORITARIO', 'min_days': 0, 'max_days': 14, 'slots': 3},
    'MEDIA':   {'emoji': '\U0001f7e1', 'label': 'NORMALE', 'min_days': 0, 'max_days': 21, 'slots': 3},
    'BASSA':   {'emoji': '\U0001f7e2', 'label': 'BASSA', 'min_days': 14, 'max_days': 42, 'slots': 3},
}

GIORNI = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
MESI = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic']
GIORNI_LAVORATIVI = [0, 1, 2]  # Solo Lun, Mar, Mer (consegna auto)

def genera_orari_giornata(weekday):
    ap_h, ap_m = map(int, ORARIO_APERTURA.split(':'))
    pp_h, pp_m = map(int, PAUSA_INIZIO.split(':'))
    pf_h, pf_m = map(int, PAUSA_FINE.split(':'))
    if weekday == 5:
        ch_h, ch_m = map(int, SABATO_CHIUSURA.split(':'))
    else:
        ch_h, ch_m = map(int, ORARIO_CHIUSURA.split(':'))
    orari = []
    cur_h, cur_m = ap_h, ap_m
    while cur_h < ch_h or (cur_h == ch_h and cur_m < ch_m):
        if cur_h >= pp_h and cur_h < pf_h:
            cur_h, cur_m = pf_h, pf_m
            continue
        orari.append(str(cur_h).zfill(2) + ':' + str(cur_m).zfill(2))
        cur_m += DURATA_SLOT
        if cur_m >= 60:
            cur_h += cur_m // 60
            cur_m = cur_m % 60
    return orari

SLOT_DAYS_AHEAD = 7

def get_busy_times(time_min, time_max):
    """Controlla Google Calendar e ritorna gli intervalli occupati."""
    service = get_calendar_service()
    if not service:
        return []
    try:
        body = {
            'timeMin': time_min,
            'timeMax': time_max,
            'items': [{'id': GOOGLE_CALENDAR_ID}],
            'timeZone': 'Europe/Rome',
        }
        result = service.freebusy().query(body=body).execute()
        busy = result.get('calendars', {}).get(GOOGLE_CALENDAR_ID, {}).get('busy', [])
        parsed = []
        for b in busy:
            start = datetime.fromisoformat(b['start'])
            end = datetime.fromisoformat(b['end'])
            parsed.append((start, end))
        return parsed
    except Exception as e:
        logger.error('Errore freebusy Calendar: ' + str(e))
        return []

def is_slot_free(slot_start, slot_end, busy_times):
    """Verifica che lo slot non si sovrapponga con nessun intervallo occupato."""
    for busy_start, busy_end in busy_times:
        if slot_start < busy_end and slot_end > busy_start:
            return False
    return True

def _distribuisci_slot(slots, n):
    """Distribuisce N slot su giorni diversi quando possibile, con orari ben spaziati."""
    if len(slots) <= n:
        return slots
    # Raggruppa per data
    by_date = {}
    for s in slots:
        by_date.setdefault(s['date'], []).append(s)
    dates = sorted(by_date.keys())
    selected = []
    # Prima passata: uno slot per giorno (prendi orario centrale della giornata)
    for d in dates:
        if len(selected) >= n:
            break
        day_slots = by_date[d]
        mid = len(day_slots) // 2
        selected.append(day_slots[mid])
    # Se servono altri slot, prendi da giorni gia' usati (orari diversi)
    if len(selected) < n:
        for d in dates:
            if len(selected) >= n:
                break
            day_slots = by_date[d]
            for s in day_slots:
                if s not in selected and len(selected) < n:
                    selected.append(s)
    selected.sort(key=lambda s: (s['date'], s['time']))
    return selected


def genera_slot(priority, all_slots=False):
    config = PRIORITY_CONFIG[priority]
    min_days = config.get('min_days', 0)
    max_days = config['max_days']
    now = datetime.now(TZ_ROME)
    date_start = (now + timedelta(days=min_days)).strftime('%Y-%m-%d')
    date_end = (now + timedelta(days=max_days)).strftime('%Y-%m-%d')
    time_min = (now + timedelta(days=min_days)).isoformat()
    time_max = (now + timedelta(days=max_days + 1)).isoformat()
    busy_times = get_busy_times(time_min, time_max)
    # Conta prenotazioni per settimana per rispettare il limite
    weekly_counts = db.count_bookings_by_week(date_start, date_end)
    slots = []
    for delta in range(min_days, max_days + 1):
        date = now + timedelta(days=delta)
        if date.weekday() not in GIORNI_LAVORATIVI:
            continue
        # Controlla limite settimanale (max 3 appuntamenti lun-dom)
        week_start = (date - timedelta(days=date.weekday())).strftime('%Y-%m-%d')
        if weekly_counts.get(week_start, 0) >= MAX_APPUNTAMENTI_SETTIMANA:
            continue
        for orario in genera_orari_giornata(date.weekday()):
            h, m = map(int, orario.split(':'))
            slot_start = datetime(date.year, date.month, date.day, h, m, tzinfo=TZ_ROME)
            if slot_start <= now:
                continue
            slot_end = slot_start + timedelta(minutes=DURATA_SLOT)
            if not is_slot_free(slot_start, slot_end, busy_times):
                continue
            si, ei = slot_start.isoformat(), slot_end.isoformat()
            g = GIORNI[date.weekday()]
            me = MESI[date.month - 1]
            slots.append({
                'display': g + ' ' + str(date.day) + ' ' + me + ' ore ' + orario,
                'date': date.strftime('%Y-%m-%d'), 'time': orario,
                'datetime_start': si, 'datetime_end': ei,
            })
    if all_slots:
        return slots
    n = config['slots']
    return _distribuisci_slot(slots, n)

CATEGORY_LABELS = {
    'motore': 'Motore', 'trasmissione': 'Trasmissione', 'freni': 'Freni',
    'sterzo': 'Sterzo', 'sospensioni': 'Sospensioni',
    'impianto_elettrico': 'Impianto Elettrico', 'climatizzazione': 'Climatizzazione',
    'carrozzeria': 'Carrozzeria', 'pneumatici': 'Pneumatici', 'luci': 'Luci',
    'tergicristalli': 'Tergicristalli', 'batteria': 'Batteria', 'scarico': 'Scarico',
    'altro': 'Altro',
}

def _find_preferred_slot(slots, preferred_datetime):
    """Cerca tra gli slot generati quello che corrisponde alla data/ora preferita dal cliente."""
    if not preferred_datetime or not slots:
        return None
    try:
        pref = datetime.strptime(preferred_datetime, '%Y-%m-%d %H:%M')
    except (ValueError, TypeError):
        return None
    pref_date = pref.strftime('%Y-%m-%d')
    pref_time = pref.strftime('%H:%M')
    # Cerca corrispondenza esatta
    for slot in slots:
        if slot['date'] == pref_date and slot['time'] == pref_time:
            return slot
    # Se l'orario esatto non c'e', cerca lo slot piu' vicino nello stesso giorno
    same_day = [s for s in slots if s['date'] == pref_date]
    if same_day:
        return min(same_day, key=lambda s: abs(int(s['time'].replace(':', '')) - int(pref_time.replace(':', ''))))
    return None

def formatta_triage(triage):
    pri = triage['priority']
    config = PRIORITY_CONFIG[pri]
    preferred = triage.get('preferred_datetime')
    # Se c'e' un orario preferito, genera tutti gli slot per cercare la corrispondenza
    all_slots = genera_slot(pri, all_slots=True) if preferred else None
    slots = genera_slot(pri)
    msg = config['emoji'] + ' *VALUTAZIONE: Priorita\' ' + config['label'] + '*\n\n'
    if pri == 'CRITICA':
        msg += '\u26a0\ufe0f *ATTENZIONE: Non utilizzi il veicolo se non strettamente necessario.*\n'
        msg += '\U0001f4de In caso di emergenza ci chiami al ' + TELEFONO + '\n\n'
    msg += '\U0001f4cb *Problema:* ' + triage['summary'] + '\n'
    msg += '\U0001f4a1 *Consiglio:* ' + triage['recommendation'] + '\n\n'
    if not slots:
        msg += '\u26a0\ufe0f Nessuno slot disponibile al momento.\n'
        msg += 'Ci contatti al ' + TELEFONO + ' per fissare un appuntamento.'
        return msg, []
    # Se il cliente ha indicato un orario preferito, usa direttamente quello
    if preferred and all_slots:
        matched = _find_preferred_slot(all_slots, preferred)
        if matched:
            msg += '\U0001f4c5 Perfetto, abbiamo disponibilita\' per *' + matched['display'] + '*.\n\n'
            msg += 'Confermiamo questo orario?'
            return msg, [matched]
    msg += '\U0001f504 L\'auto restera\' in officina per un ricovero di 24-72 ore.\n\n'
    msg += '\U0001f4c5 *Quando puo\' portare l\'auto?*\n'
    for i, slot in enumerate(slots, 1):
        msg += '  *' + str(i) + '.* ' + slot['display'] + '\n'
    msg += '\n\U0001f449 Risponda con il *numero* dello slot (es. "1")'
    return msg, slots

def conferma_prenotazione(phone, pending):
    slot = pending['slot']
    nome = pending.get('nome_cliente', '')
    auto = pending.get('auto_cliente', '')
    triage = db.get_latest_triage(phone)
    triage_data = dict(triage) if triage else {}
    triage_id = triage_data.get('id')
    triage_data['nome_cliente'] = nome
    triage_data['auto_cliente'] = auto
    google_event_id = crea_evento_calendar(slot, nome, triage_data)
    booking_id = db.create_booking(phone, slot, triage_id, nome, auto, google_event_id)
    # Collega le foto inviate durante il triage a questa prenotazione
    db.link_photos_to_booking(phone, booking_id)
    msg = '\u2705 *Appuntamento Confermato!*\n\n'
    msg += '\U0001f464 *Cliente:* ' + nome + '\n'
    msg += '\U0001f697 *Veicolo:* ' + auto + '\n'
    msg += '\U0001f4c5 *Consegna:* ' + slot['display'] + '\n'
    msg += '\U0001f504 *Ricovero:* l\'auto restera\' in officina 24-72 ore. La contatteremo quando sara\' pronta.\n'
    msg += '\U0001f4cd ' + INDIRIZZO + '\n\U0001f4de ' + TELEFONO
    msg += '\n\nRicevera\' un promemoria il giorno prima.\nGrazie e a presto! \U0001f44b'
    # Invia messaggio conferma, poi dopo un attimo il messaggio feedback separato
    send_whatsapp_message(phone, msg)
    feedback_msg = '\U0001f4dd L\'assistente WhatsApp le e\' stato utile? Risponda da *1* (per niente) a *5* (molto utile).'
    # Salva stato feedback nella conversazione
    feedback_pending = {'state': 'waiting_feedback', 'booking_id': booking_id}
    messages, _ = db.get_conversation(phone)
    db.save_conversation(phone, messages, feedback_pending)
    if WHATSAPP_OFFICINA:
        pri = triage_data.get('priority', '')
        if pri == 'CRITICA':
            notifica = '\U0001f6a8\U0001f6a8 *PRENOTAZIONE URGENTE* \U0001f6a8\U0001f6a8\n\n'
        elif pri == 'ALTA':
            notifica = '\u26a0\ufe0f *Nuova Prenotazione PRIORITARIA*\n\n'
        else:
            notifica = '\U0001f4cb *Nuova Prenotazione*\n\n'
        notifica += '\U0001f464 *Cliente:* ' + nome + '\n'
        notifica += '\U0001f697 *Veicolo:* ' + auto + '\n'
        notifica += '\U0001f4de *Telefono:* ' + phone + '\n'
        notifica += '\U0001f4c5 *Appuntamento:* ' + slot['display'] + '\n'
        if pri:
            pri_config = PRIORITY_CONFIG.get(pri, {})
            pri_label = pri_config.get('emoji', '') + ' ' + pri_config.get('label', pri)
            notifica += '\U0001f527 *Priorita\':* ' + pri_label + '\n'
        if triage_data.get('category'):
            label = CATEGORY_LABELS.get(triage_data['category'], triage_data['category'])
            notifica += '\U0001f3f7\ufe0f *Categoria:* ' + label + '\n'
        if triage_data.get('summary'):
            notifica += '\U0001f4dd *Problema:* ' + triage_data['summary'] + '\n'
        if triage_data.get('recommendation'):
            notifica += '\U0001f4a1 *Consiglio:* ' + triage_data['recommendation'] + '\n'
        if triage_data.get('emotional_note'):
            notifica += '\U0001f9e0 *Stato emotivo:* ' + triage_data['emotional_note']
        if pri in ('CRITICA', 'ALTA'):
            notifica += '\n\n\u26a1 *Richiede attenzione immediata*'
        logger.info('Invio notifica a ' + WHATSAPP_OFFICINA)
        result = send_whatsapp_message(WHATSAPP_OFFICINA, notifica)
        logger.info('Notifica inviata: ' + str(result))
    return feedback_msg

def gestisci_prenotazione(phone, scelta, pending_data):
    # pending_data puo' essere:
    # - lista di slot (scelta slot)
    # - dict con state (raccolta dati cliente)
    if isinstance(pending_data, list):
        # Fase 1: scelta slot
        try:
            idx = int(scelta.strip()) - 1
            if 0 <= idx < len(pending_data):
                slot = pending_data[idx]
                new_pending = {'state': 'waiting_name', 'slot': slot}
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, new_pending)
                return '\U0001f464 Per completare la prenotazione, mi dica *nome e cognome*.'
            else:
                return '\u26a0\ufe0f Scelta non valida. Risponda con un numero da 1 a ' + str(len(pending_data)) + '.'
        except ValueError:
            return None
    elif isinstance(pending_data, dict):
        state = pending_data.get('state', '')
        if state == 'waiting_name':
            # Fase 2: raccolta nome
            nome = scelta.strip()
            if len(nome) < 2:
                return '\u26a0\ufe0f Per favore, inserisca nome e cognome validi.'
            pending_data['nome_cliente'] = nome
            pending_data['state'] = 'waiting_car'
            messages, _ = db.get_conversation(phone)
            db.save_conversation(phone, messages, pending_data)
            return '\U0001f697 Quale veicolo porta in officina? (marca, modello e targa)\nEs: _Fiat Punto AB123CD_'
        elif state == 'waiting_car':
            # Fase 3: raccolta auto → conferma
            auto = scelta.strip()
            if len(auto) < 3:
                return '\u26a0\ufe0f Per favore, inserisca i dati del veicolo (marca, modello e targa).'
            pending_data['auto_cliente'] = auto
            return conferma_prenotazione(phone, pending_data)
        elif state == 'waiting_feedback':
            # Fase 4: voto feedback 1-5
            try:
                rating = int(scelta.strip())
                if rating < 1 or rating > 5:
                    return '\u26a0\ufe0f Risponda con un numero da *1* a *5*.'
            except ValueError:
                return '\u26a0\ufe0f Risponda con un numero da *1* a *5*.'
            booking_id = pending_data.get('booking_id')
            if rating >= 4:
                db.save_feedback(phone, booking_id, rating)
                db.clear_conversation(phone)
                return '\U0001f64f Grazie mille per il suo feedback! Siamo felici di esserle stati utili.\nA presto! \U0001f44b'
            else:
                # Rating basso: chiedi cosa migliorare
                pending_data['state'] = 'waiting_feedback_detail'
                pending_data['rating'] = rating
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, pending_data)
                return '\U0001f4ac Ci dispiace non averla soddisfatta pienamente. Ci dica cosa potremmo migliorare, il suo parere e\' prezioso per noi.'
        elif state == 'waiting_feedback_detail':
            # Fase 5: commento libero dopo voto basso
            comment = scelta.strip()
            booking_id = pending_data.get('booking_id')
            rating = pending_data.get('rating', 0)
            db.save_feedback(phone, booking_id, rating, comment)
            db.clear_conversation(phone)
            return '\U0001f64f Grazie per il suo feedback, ne terremo conto per migliorare il servizio.\nA presto! \U0001f44b'
        elif state == 'reschedule_verify_plate':
            # Verifica targa prima di procedere
            targa_input = scelta.strip().upper().replace(' ', '').replace('-', '')
            auto_prenotata = pending_data.get('reschedule_auto', '')
            # Estrai targa dalla stringa auto (es. "Fiat Punto AB123CD")
            targa_prenotata = auto_prenotata.upper().replace(' ', '').replace('-', '')
            if targa_input and len(targa_input) >= 4 and targa_input in targa_prenotata:
                # Targa corretta, mostra prenotazione
                nome = pending_data.get('reschedule_nome', '')
                slot_display = pending_data.get('reschedule_slot_display', '')
                msg = '\u2705 *Prenotazione trovata:*\n\n'
                if nome:
                    msg += '\U0001f464 *Cliente:* ' + nome + '\n'
                if auto_prenotata:
                    msg += '\U0001f697 *Veicolo:* ' + auto_prenotata + '\n'
                msg += '\U0001f4c5 *Appuntamento:* ' + slot_display + '\n\n'
                msg += 'Cosa desidera fare?\n'
                msg += '  *1.* Spostare l\'appuntamento\n'
                msg += '  *2.* Cancellare l\'appuntamento'
                pending_data['state'] = 'reschedule_confirm'
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, pending_data)
                return msg
            else:
                db.clear_conversation(phone)
                return '\u26a0\ufe0f La targa non corrisponde alla prenotazione trovata.\n\nSe ha bisogno di assistenza, ci scriva pure.'
        elif state == 'reschedule_confirm':
            # Spostamento: cliente conferma se spostare o cancellare
            risposta = scelta.strip().lower()
            if risposta in ['1', 'spostare', 'sposta']:
                # Mostra slot disponibili
                booking_id = pending_data['reschedule_booking_id']
                priority = pending_data.get('reschedule_priority', 'MEDIA')
                slots = genera_slot(priority)
                if not slots:
                    db.clear_conversation(phone)
                    return '\u26a0\ufe0f Nessuno slot disponibile al momento. Riprovera\' piu\' tardi.'
                msg = '\U0001f4c5 *Slot disponibili:*\n'
                for i, slot in enumerate(slots, 1):
                    msg += '  *' + str(i) + '.* ' + slot['display'] + '\n'
                msg += '\n\U0001f449 Risponda con il *numero* dello slot desiderato.'
                pending_data['state'] = 'reschedule_waiting_slot'
                pending_data['reschedule_slots'] = slots
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, pending_data)
                return msg
            elif risposta in ['2', 'cancellare', 'cancella']:
                # Cancella appuntamento
                booking_id = pending_data['reschedule_booking_id']
                old_event_id = db.get_booking_google_event_id(booking_id)
                cancella_evento_calendar(old_event_id)
                db.cancel_booking(booking_id)
                db.clear_conversation(phone)
                return '\u2705 *Appuntamento cancellato.*\n\nSe avra\' bisogno in futuro, ci scriva pure qui in chat.\nA presto! \U0001f44b'
            else:
                return '\u26a0\ufe0f Risponda con *1* per spostare o *2* per cancellare.'
        elif state == 'reschedule_waiting_slot':
            # Spostamento: scelta nuovo slot
            try:
                idx = int(scelta.strip()) - 1
                slots = pending_data.get('reschedule_slots', [])
                if 0 <= idx < len(slots):
                    new_slot = slots[idx]
                    booking_id = pending_data['reschedule_booking_id']
                    nome = pending_data.get('reschedule_nome', '')
                    auto = pending_data.get('reschedule_auto', '')
                    # Cancella vecchio evento Calendar
                    old_event_id = db.get_booking_google_event_id(booking_id)
                    cancella_evento_calendar(old_event_id)
                    # Crea nuovo evento Calendar
                    triage = db.get_latest_triage(phone)
                    triage_data = dict(triage) if triage else {}
                    triage_data['auto_cliente'] = auto
                    new_event_id = crea_evento_calendar(new_slot, nome, triage_data)
                    # Aggiorna booking nel DB
                    db.update_booking(booking_id, new_slot, new_event_id)
                    db.clear_conversation(phone)
                    msg = '\u2705 *Appuntamento Spostato!*\n\n'
                    msg += '\U0001f464 *Cliente:* ' + nome + '\n'
                    msg += '\U0001f697 *Veicolo:* ' + auto + '\n'
                    msg += '\U0001f4c5 *Nuovo appuntamento:* ' + new_slot['display'] + '\n'
                    msg += '\U0001f4cd ' + INDIRIZZO + '\n\U0001f4de ' + TELEFONO
                    msg += '\n\nA presto! \U0001f44b'
                    return msg
                else:
                    return '\u26a0\ufe0f Scelta non valida. Risponda con un numero da 1 a ' + str(len(slots)) + '.'
            except ValueError:
                return '\u26a0\ufe0f Risponda con il *numero* dello slot desiderato.'
    return None

RESCHEDULE_KEYWORDS = ['spostare', 'sposta', 'modificare', 'modifica', 'cambiare', 'cambia',
                        'riprogrammare', 'riprogramma', 'posticipare', 'posticipa', 'anticipare',
                        'anticipa', 'spostamento', 'cambio appuntamento', 'modifica appuntamento',
                        'cancellare', 'cancella', 'disdire', 'disdetta', 'annullare', 'annulla']

def is_reschedule_intent(msg):
    msg_lower = msg.lower()
    return any(kw in msg_lower for kw in RESCHEDULE_KEYWORDS) and any(
        w in msg_lower for w in ['appuntamento', 'prenotazione', 'data', 'orario', 'giorno', 'visita'])

def avvia_flusso_modifica(phone, messages):
    """Cerca prenotazione attiva per telefono e avvia il flusso spostamento/cancellazione."""
    booking = db.find_active_booking_by_phone(phone)
    if not booking:
        return '\u26a0\ufe0f Non abbiamo trovato prenotazioni attive associate a questo numero.\n\nSe desidera prenotare un nuovo appuntamento, mi descriva il problema alla sua auto.'
    reschedule_pending = {
        'state': 'reschedule_verify_plate',
        'reschedule_booking_id': booking['id'],
        'reschedule_nome': booking.get('nome_cliente', '') or '',
        'reschedule_auto': booking.get('auto_cliente', '') or '',
        'reschedule_slot_display': booking.get('slot_display', '') or '',
        'reschedule_priority': booking.get('priority', 'MEDIA'),
    }
    db.save_conversation(phone, messages or [], reschedule_pending)
    return '\U0001f50d Per sicurezza, mi indichi la *targa* del veicolo prenotato.'

def _save_photo_for_booking(phone, b64_data, media_type):
    """Salva la foto nel DB (senza booking_id, verra' collegata dopo)."""
    try:
        db.save_photo(phone, b64_data, media_type)
        logger.info('Foto salvata per ' + phone)
    except Exception as e:
        logger.error('Errore salvataggio foto: ' + str(e))


WELCOME_MSG = ('Benvenuto nell\'assistenza *' + NOME + '*! \U0001f697\n'
    'Come posso aiutarla? Scelga un\'opzione o mi descriva il suo problema:\n\n'
    '1\ufe0f\u20e3 Ho un problema con la mia auto\n'
    '2\ufe0f\u20e3 Vorrei spostare o cancellare un appuntamento\n'
    '3\ufe0f\u20e3 Informazioni e orari officina\n\n'
    'Puo\' anche scrivermi liberamente il suo problema e lo gestiro\' subito!')

INFO_MSG = ('\U0001f3e2 *' + NOME + '*\n\n'
    '\U0001f4cd *Indirizzo:* ' + INDIRIZZO + '\n'
    '\U0001f4de *Telefono:* ' + TELEFONO + '\n\n'
    '\U0001f552 *Orari di apertura:*\n'
    '  Lun-Ven: ' + ORARIO_APERTURA + ' - ' + ORARIO_CHIUSURA + '\n'
    '  Pausa pranzo: ' + PAUSA_INIZIO + ' - ' + PAUSA_FINE + '\n'
    '  Sabato: ' + ORARIO_APERTURA + ' - ' + SABATO_CHIUSURA + '\n'
    '  Domenica: Chiuso\n\n'
    'Per qualsiasi necessita\', ci scriva pure qui in chat!')

def process_message(from_number, incoming_msg, image=None):
    logger.info('\U0001f4e9 ' + from_number + ': ' + incoming_msg + (' [+foto]' if image else ''))
    if incoming_msg.lower() in ['reset', 'ricomincia', 'riparti']:
        db.clear_conversation(from_number)
        claude_msgs = [{'role': 'user', 'content': incoming_msg}, {'role': 'assistant', 'content': WELCOME_MSG}]
        db.save_conversation(from_number, claude_msgs, None)
        return WELCOME_MSG
    messages, pending_slots = db.get_conversation(from_number)
    # Prima conversazione: nessun messaggio precedente e nessun flusso in corso
    if not messages and not pending_slots:
        msg_stripped = incoming_msg.strip()
        # Se l'utente sceglie direttamente un'opzione del menu, mostra welcome + gestisci subito
        if msg_stripped in ['1', '2', '3']:
            claude_msgs = [{'role': 'user', 'content': incoming_msg}, {'role': 'assistant', 'content': WELCOME_MSG}]
            db.save_conversation(from_number, claude_msgs, None)
            send_whatsapp_message(from_number, WELCOME_MSG)
            if msg_stripped == '2':
                return avvia_flusso_modifica(from_number, claude_msgs)
            if msg_stripped == '3':
                return INFO_MSG
            incoming_msg = 'Ho un problema con la mia auto'
            messages = claude_msgs
            # Prosegui al flusso Claude sotto
        else:
            claude_msgs = [{'role': 'user', 'content': incoming_msg}, {'role': 'assistant', 'content': WELCOME_MSG}]
            db.save_conversation(from_number, claude_msgs, None)
            return WELCOME_MSG
    # Gestione risposte rapide dal menu
    msg_stripped = incoming_msg.strip()
    if not pending_slots and msg_stripped == '1':
        incoming_msg = 'Ho un problema con la mia auto'
        # Lascia proseguire al flusso Claude normale sotto
    elif not pending_slots and msg_stripped == '2':
        return avvia_flusso_modifica(from_number, messages)
    elif not pending_slots and msg_stripped == '3':
        return INFO_MSG
    # Rileva intent spostamento/cancellazione (solo se non c'è già un flusso in corso)
    if not pending_slots and is_reschedule_intent(incoming_msg):
        return avvia_flusso_modifica(from_number, messages)
    if pending_slots:
        result = gestisci_prenotazione(from_number, incoming_msg, pending_slots)
        if result:
            return result
    claude_msgs = [m for m in messages if isinstance(m, dict) and 'role' in m]
    # Costruisci il contenuto del messaggio utente (testo + eventuale immagine)
    if image:
        b64_data, media_type = image
        user_content = [
            {'type': 'image', 'source': {'type': 'base64', 'media_type': media_type, 'data': b64_data}},
            {'type': 'text', 'text': incoming_msg},
        ]
        # Salva nella conversazione solo il testo (le immagini non si ri-inviano nelle conversazioni successive)
        claude_msgs.append({'role': 'user', 'content': incoming_msg})
        # Per la chiamata a Claude, usa i messaggi precedenti + l'ultimo con immagine
        claude_msgs_for_api = claude_msgs[:-1] + [{'role': 'user', 'content': user_content}]
        # Salva riferimento foto per il DB
        _save_photo_for_booking(from_number, b64_data, media_type)
    else:
        claude_msgs.append({'role': 'user', 'content': incoming_msg})
        claude_msgs_for_api = claude_msgs
    try:
        response = claude_client.messages.create(
            model='claude-sonnet-4-6', max_tokens=1000,
            system=SYSTEM_PROMPT, messages=claude_msgs_for_api,
        )
        reply = response.content[0].text
    except Exception as e:
        logger.error('Claude errore: ' + str(e))
        return 'Mi scusi, problema tecnico. Riprovi tra qualche secondo.'
    reply_text = reply
    new_pending = None
    try:
        match = re.search(r'\{[\s\S]*"triage_complete"\s*:\s*true[\s\S]*\}', reply)
        if match:
            triage = json.loads(match.group(0))
            db.save_triage(from_number, triage)
            reply_text, slots = formatta_triage(triage)
            new_pending = slots
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning('Parsing: ' + str(e))
    claude_msgs.append({'role': 'assistant', 'content': reply})
    db.save_conversation(from_number, claude_msgs, new_pending)
    return reply_text

# --- FLASK ---
app = Flask(__name__)

@app.route('/', methods=['GET'])
def health():
    return {'status': 'ok', 'service': NOME}, 200

@app.route('/webhook', methods=['GET'])
def verify():
    if request.args.get('hub.mode') == 'subscribe' and request.args.get('hub.verify_token') == META_VERIFY_TOKEN:
        return request.args.get('hub.challenge', ''), 200
    return 'Forbidden', 403

def verify_signature(req_obj):
    if not META_APP_SECRET:
        return True
    signature = req_obj.headers.get('X-Hub-Signature-256', '')
    if not signature.startswith('sha256='):
        return False
    expected = hmac.new(META_APP_SECRET.encode(), req_obj.get_data(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature[7:], expected)

@app.route('/webhook', methods=['POST'])
def webhook():
    if not verify_signature(request):
        logger.warning('Webhook firma non valida - richiesta rifiutata')
        return 'Forbidden', 403
    data = request.get_json()
    try:
        msgs = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('messages', [])
        if not msgs:
            return 'OK', 200
        msg = msgs[0]
        msg_id = msg.get('id', '')
        phone = msg.get('from', '')

        if db.is_duplicate(msg_id):
            logger.info('Duplicato ignorato: ' + msg_id)
            return 'OK', 200

        contacts = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('contacts', [])
        if contacts:
            profile_name = contacts[0].get('profile', {}).get('name', '')
            if profile_name and phone:
                db.upsert_customer(phone, profile_name)

        msg_type = msg.get('type')
        text = None
        image_data = None  # (base64, media_type) se presente

        if msg_type == 'text':
            text = msg.get('text', {}).get('body', '').strip()
        elif msg_type == 'audio' and openai_client:
            audio_id = msg.get('audio', {}).get('id', '')
            if audio_id:
                text = transcribe_audio(audio_id)
                if not text:
                    send_whatsapp_message(phone, 'Non sono riuscito a capire il vocale. Puoi ripetere o scrivere?')
        elif msg_type == 'image':
            img = msg.get('image', {})
            image_id = img.get('id', '')
            caption = img.get('caption', '').strip()
            text = caption if caption else 'Il cliente ha inviato una foto.'
            if image_id:
                b64, media_type = download_image_as_base64(image_id)
                if b64:
                    image_data = (b64, media_type)
                else:
                    send_whatsapp_message(phone, 'Non sono riuscito a scaricare la foto. Puo\' riprovare?')

        if text:
            img = image_data  # cattura per il thread
            def process_and_send():
                lock = get_user_lock(phone)
                with lock:
                    reply = process_message(phone, text, image=img)
                    send_whatsapp_message(phone, reply)
            threading.Thread(target=process_and_send, daemon=True).start()
        elif msg_type not in ('text', 'audio', 'image'):
            send_whatsapp_message(phone, 'Posso elaborare messaggi di testo, vocali e foto.')
    except Exception as e:
        logger.error('Webhook errore: ' + str(e))
    return 'OK', 200

app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'chatbot-officina-secret-key-2024')

# --- PAGINA CAPOFFICINA ---

ADMIN_LOGIN_HTML = '''
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - ''' + NOME + '''</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: #f0f2f5; display: flex; justify-content: center; align-items: center; min-height: 100vh; }
        .login-box { background: white; padding: 40px; border-radius: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                     width: 90%%; max-width: 400px; text-align: center; }
        .login-box h1 { font-size: 24px; margin-bottom: 8px; color: #1a1a1a; }
        .login-box p { color: #666; margin-bottom: 24px; }
        input[type="password"] { width: 100%%; padding: 12px 16px; border: 1px solid #ddd; border-radius: 8px;
                                  font-size: 16px; margin-bottom: 16px; }
        button { width: 100%%; padding: 12px; background: #25D366; color: white; border: none; border-radius: 8px;
                 font-size: 16px; font-weight: bold; cursor: pointer; }
        button:hover { background: #1da851; }
        .error { color: #e74c3c; margin-bottom: 16px; font-size: 14px; }
    </style>
</head>
<body>
    <div class="login-box">
        <h1>&#x1f527; ''' + NOME + '''</h1>
        <p>Area Capofficina</p>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="POST">
            <input type="password" name="password" placeholder="Password" required autofocus>
            <button type="submit">Accedi</button>
        </form>
    </div>
</body>
</html>
'''

ADMIN_PAGE_HTML = '''
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Capofficina - ''' + NOME + '''</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: #f0f2f5; }
        .header { background: #075E54; color: white; padding: 16px 20px; display: flex;
                  justify-content: space-between; align-items: center; }
        .header h1 { font-size: 20px; }
        .header a { color: white; text-decoration: none; font-size: 14px; opacity: 0.8; }
        .header a:hover { opacity: 1; }
        .container { max-width: 800px; margin: 20px auto; padding: 0 16px; }
        .card { background: white; border-radius: 12px; box-shadow: 0 1px 4px rgba(0,0,0,0.1);
                margin-bottom: 12px; padding: 16px; display: flex; justify-content: space-between;
                align-items: center; flex-wrap: wrap; gap: 12px; }
        .card-info { flex: 1; min-width: 200px; }
        .card-info .name { font-weight: bold; font-size: 16px; color: #1a1a1a; }
        .card-info .details { font-size: 14px; color: #666; margin-top: 4px; }
        .card-info .slot { font-size: 14px; color: #075E54; font-weight: 500; margin-top: 4px; }
        .card-info .problem { font-size: 13px; color: #888; margin-top: 4px; font-style: italic; }
        .card-photos { display: flex; gap: 6px; margin-top: 8px; flex-wrap: wrap; }
        .card-photos img { width: 60px; height: 60px; object-fit: cover; border-radius: 6px;
                           border: 1px solid #ddd; cursor: pointer; transition: transform 0.2s; }
        .card-photos img:hover { transform: scale(1.1); }
        .photo-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
                       background: rgba(0,0,0,0.8); z-index: 1000; justify-content: center; align-items: center; }
        .photo-modal img { max-width: 90%; max-height: 90%; border-radius: 8px; }
        .photo-modal.active { display: flex; }
        .btn-group { display: flex; flex-direction: column; gap: 8px; }
        .btn-avvisa { padding: 10px 20px; background: #25D366; color: white; border: none;
                      border-radius: 8px; font-size: 14px; font-weight: bold; cursor: pointer;
                      white-space: nowrap; }
        .btn-avvisa:hover { background: #1da851; }
        .btn-avvisa:disabled { background: #ccc; cursor: not-allowed; }
        .btn-avvisa.sent { background: #27ae60; }
        .btn-non-pronta { padding: 10px 20px; background: #e67e22; color: white; border: none;
                          border-radius: 8px; font-size: 14px; font-weight: bold; cursor: pointer;
                          white-space: nowrap; }
        .btn-non-pronta:hover { background: #d35400; }
        .btn-non-pronta:disabled { background: #ccc; cursor: not-allowed; }
        .empty { text-align: center; padding: 60px 20px; color: #999; font-size: 16px; }
        .flash { max-width: 800px; margin: 12px auto; padding: 12px 20px; border-radius: 8px;
                 font-size: 14px; }
        .flash.success { background: #d4edda; color: #155724; }
        .flash.error { background: #f8d7da; color: #721c24; }
        .priority { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px;
                    font-weight: bold; margin-left: 8px; }
        .priority.CRITICA { background: #ffe0e0; color: #c0392b; }
        .priority.ALTA { background: #fff3e0; color: #e67e22; }
        .priority.MEDIA { background: #fff9e0; color: #f39c12; }
        .priority.BASSA { background: #e0f7e0; color: #27ae60; }
    </style>
</head>
<body>
    <div class="header">
        <h1>&#x1f527; Capofficina</h1>
        <a href="/admin/logout">Esci</a>
    </div>
    {% if flash_msg %}
    <div class="flash {{ flash_type }}">{{ flash_msg }}</div>
    {% endif %}
    <div class="container">
        {% if bookings %}
            {% for b in bookings %}
            <div class="card">
                <div class="card-info">
                    <div class="name">
                        {{ b.nome_cliente or b.customer_name or 'Cliente' }}
                        {% if b.priority %}
                        <span class="priority {{ b.priority }}">{{ b.priority }}</span>
                        {% endif %}
                    </div>
                    <div class="details">&#x1f697; {{ b.auto_cliente or 'N/D' }}</div>
                    <div class="slot">&#x1f4c5; {{ b.slot_display or 'N/D' }}</div>
                    {% if b.summary %}
                    <div class="problem">&#x1f4dd; {{ b.summary }}</div>
                    {% endif %}
                    {% if b.photos %}
                    <div class="card-photos">
                        {% for p in b.photos %}
                        <img src="/admin/photo/{{ p.id }}" alt="Foto cliente"
                             onclick="document.getElementById('modal').classList.add('active'); document.getElementById('modal-img').src=this.src;">
                        {% endfor %}
                    </div>
                    {% endif %}
                </div>
                <div class="btn-group">
                    <form method="POST" action="/admin/avvisa">
                        <input type="hidden" name="booking_id" value="{{ b.id }}">
                        <input type="hidden" name="phone" value="{{ b.phone }}">
                        <input type="hidden" name="nome" value="{{ b.nome_cliente or b.customer_name or 'Cliente' }}">
                        <input type="hidden" name="auto" value="{{ b.auto_cliente or '' }}">
                        <button type="submit" class="btn-avvisa"
                                onclick="this.disabled=true; this.innerText='Invio...'; this.form.submit();">
                            &#x2705; Auto Pronta
                        </button>
                    </form>
                    <form method="POST" action="/admin/non-pronta">
                        <input type="hidden" name="booking_id" value="{{ b.id }}">
                        <input type="hidden" name="phone" value="{{ b.phone }}">
                        <input type="hidden" name="nome" value="{{ b.nome_cliente or b.customer_name or 'Cliente' }}">
                        <input type="hidden" name="auto" value="{{ b.auto_cliente or '' }}">
                        <button type="submit" class="btn-non-pronta"
                                onclick="this.disabled=true; this.innerText='Invio...'; this.form.submit();">
                            &#x23f3; Auto Non Pronta
                        </button>
                    </form>
                </div>
            </div>
            {% endfor %}
        {% else %}
            <div class="empty">Nessuna prenotazione attiva al momento.</div>
        {% endif %}
    </div>
    <div class="photo-modal" id="modal" onclick="this.classList.remove('active');">
        <img id="modal-img" src="">
    </div>
</body>
</html>
'''


@app.route('/admin', methods=['GET', 'POST'])
def admin_login():
    if session.get('admin'):
        return redirect('/admin/dashboard')
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['admin'] = True
            return redirect('/admin/dashboard')
        error = 'Password errata'
    return render_template_string(ADMIN_LOGIN_HTML, error=error)


@app.route('/admin/dashboard')
def admin_dashboard():
    if not session.get('admin'):
        return redirect('/admin')
    flash_msg = request.args.get('msg')
    flash_type = request.args.get('type', 'success')
    bookings = db.get_active_bookings()
    # Carica le foto per ogni prenotazione
    for b in bookings:
        b['photos'] = db.get_photos_for_booking(b['id'])
    return render_template_string(ADMIN_PAGE_HTML, bookings=bookings,
                                  flash_msg=flash_msg, flash_type=flash_type)


@app.route('/admin/photo/<int:photo_id>')
def admin_photo(photo_id):
    if not session.get('admin'):
        return redirect('/admin')
    from flask import Response
    photo = db.get_photo_data(photo_id)
    if not photo:
        return 'Not found', 404
    image_bytes = base64.b64decode(photo['image_data'])
    return Response(image_bytes, mimetype=photo['media_type'])


@app.route('/admin/avvisa', methods=['POST'])
def admin_avvisa():
    if not session.get('admin'):
        return redirect('/admin')
    phone = request.form.get('phone', '')
    nome = request.form.get('nome', '')
    auto = request.form.get('auto', '')
    ok, err = send_template_message(phone, TEMPLATE_NAME, [nome, auto])
    if ok:
        return redirect('/admin/dashboard?msg=Messaggio+inviato+a+' + nome + '&type=success')
    else:
        return redirect('/admin/dashboard?msg=Errore:+' + err[:80] + '&type=error')


@app.route('/admin/non-pronta', methods=['POST'])
def admin_non_pronta():
    if not session.get('admin'):
        return redirect('/admin')
    phone = request.form.get('phone', '')
    nome = request.form.get('nome', '')
    auto = request.form.get('auto', '')
    ok, err = send_template_message(phone, TEMPLATE_NON_PRONTA, [nome, auto])
    if ok:
        return redirect('/admin/dashboard?msg=Avviso+auto+non+pronta+inviato+a+' + nome + '&type=success')
    else:
        return redirect('/admin/dashboard?msg=Errore:+' + err[:80] + '&type=error')


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect('/admin')


# --- PROMEMORIA AUTOMATICI ---

def invia_promemoria():
    """Controlla prenotazioni di domani e invia promemoria ai clienti."""
    try:
        bookings = db.get_tomorrow_bookings()
        if not bookings:
            logger.info('Promemoria: nessuna prenotazione per domani')
            return
        for b in bookings:
            nome = b.get('nome_cliente', 'Cliente')
            orario = 'ore ' + b.get('slot_time', '')
            phone = b.get('phone', '')
            ok, err = send_template_message(phone, TEMPLATE_PROMEMORIA, [nome, orario])
            if ok:
                db.mark_promemoria_sent(b['id'])
                logger.info('Promemoria inviato a ' + nome + ' (' + phone + ')')
            else:
                logger.error('Promemoria fallito per ' + phone + ': ' + err)
    except Exception as e:
        logger.error('Errore invio promemoria: ' + str(e))


def scheduler_promemoria():
    """Esegue il controllo promemoria ogni ora."""
    import time
    while True:
        now = datetime.now(TZ_ROME)
        # Invia promemoria solo tra le 9 e le 20
        if 9 <= now.hour <= 20:
            invia_promemoria()
        time.sleep(3600)  # Ogni ora


# ============================================================
# API PER BRO CAR AI AGENT
# ============================================================

AGENT_API_KEY = os.environ.get('AGENT_API_KEY', 'brocar-agent-2026')


def require_agent_key(f):
    """Decorator per proteggere le API con chiave."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-Agent-Key', '') or request.args.get('key', '')
        if key != AGENT_API_KEY:
            return {'error': 'Unauthorized'}, 401
        return f(*args, **kwargs)
    return decorated


@app.route('/api/agent/kpi', methods=['GET'])
@require_agent_key
def agent_kpi():
    """KPI per la dashboard BRO CAR AI AGENT."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) as totale FROM bookings')
            totale_prenotazioni = cur.fetchone()['totale']

            cur.execute("SELECT COUNT(*) as oggi FROM bookings WHERE slot_date = CURRENT_DATE")
            prenotazioni_oggi = cur.fetchone()['oggi']

            cur.execute('SELECT COUNT(*) as totale FROM conversations')
            totale_conversazioni = cur.fetchone()['totale']

            cur.execute('SELECT COUNT(*) as attive FROM conversations WHERE active = true')
            conversazioni_attive = cur.fetchone()['attive']

            cur.execute('SELECT COUNT(DISTINCT phone) as clienti FROM customers')
            clienti_unici = cur.fetchone()['clienti']

            cur.execute('SELECT COALESCE(AVG(rating), 0) as media, COUNT(*) as totale FROM feedback')
            fb = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*) as settimana FROM bookings
                WHERE slot_date >= date_trunc('week', CURRENT_DATE)
                AND slot_date < date_trunc('week', CURRENT_DATE) + interval '7 days'
            """)
            prenotazioni_settimana = cur.fetchone()['settimana']

            cur.execute("""
                SELECT COUNT(*) as mese FROM bookings
                WHERE slot_date >= date_trunc('month', CURRENT_DATE)
                AND slot_date < date_trunc('month', CURRENT_DATE) + interval '1 month'
            """)
            prenotazioni_mese = cur.fetchone()['mese']

        return {
            'prenotazioni_totali': totale_prenotazioni,
            'prenotazioni_oggi': prenotazioni_oggi,
            'prenotazioni_settimana': prenotazioni_settimana,
            'prenotazioni_mese': prenotazioni_mese,
            'conversazioni_totali': totale_conversazioni,
            'conversazioni_attive': conversazioni_attive,
            'clienti_unici': clienti_unici,
            'feedback_medio': round(float(fb['media']), 1),
            'feedback_totale': fb['totale']
        }
    finally:
        conn.close()


@app.route('/api/agent/prenotazioni', methods=['GET'])
@require_agent_key
def agent_prenotazioni():
    """Tutte le prenotazioni."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            limit = request.args.get('limit', 50, type=int)
            cur.execute("""
                SELECT b.id, b.phone, b.nome_cliente, b.auto_cliente, b.status,
                       b.slot_date::text, b.slot_time, b.slot_display, b.created_at::text,
                       t.category, t.priority, t.summary, t.recommendation
                FROM bookings b
                LEFT JOIN triage_results t ON b.triage_id = t.id
                ORDER BY b.slot_date DESC, b.slot_time DESC
                LIMIT %s
            """, (limit,))
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/prenotazioni/oggi', methods=['GET'])
@require_agent_key
def agent_prenotazioni_oggi():
    """Prenotazioni di oggi."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.id, b.phone, b.nome_cliente, b.auto_cliente, b.status,
                       b.slot_date::text, b.slot_time, b.slot_display,
                       t.category, t.priority, t.summary
                FROM bookings b
                LEFT JOIN triage_results t ON b.triage_id = t.id
                WHERE b.slot_date = CURRENT_DATE
                ORDER BY b.slot_time ASC
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/clienti', methods=['GET'])
@require_agent_key
def agent_clienti():
    """Lista clienti con statistiche."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.phone, c.name, c.created_at::text,
                       COUNT(DISTINCT b.id) as num_prenotazioni,
                       COUNT(DISTINCT conv.id) as num_conversazioni
                FROM customers c
                LEFT JOIN bookings b ON c.phone = b.phone
                LEFT JOIN conversations conv ON c.phone = conv.phone
                GROUP BY c.phone, c.name, c.created_at
                ORDER BY c.created_at DESC
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/feedback', methods=['GET'])
@require_agent_key
def agent_feedback():
    """Lista feedback."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT f.rating, f.comment, f.created_at::text, f.phone,
                       b.nome_cliente, b.auto_cliente, b.slot_date::text, b.slot_time
                FROM feedback f
                LEFT JOIN bookings b ON f.booking_id = b.id
                ORDER BY f.created_at DESC
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/triage', methods=['GET'])
@require_agent_key
def agent_triage():
    """Lista triage."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, phone, category, priority, summary, recommendation,
                       emotional_note, created_at::text
                FROM triage_results
                ORDER BY created_at DESC
                LIMIT 50
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/chart/prenotazioni-mese', methods=['GET'])
@require_agent_key
def agent_chart_prenotazioni_mese():
    """Prenotazioni per giorno (ultimo mese)."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT slot_date::text as giorno, COUNT(*) as totale
                FROM bookings
                WHERE slot_date >= CURRENT_DATE - interval '30 days'
                GROUP BY slot_date ORDER BY slot_date
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/chart/priorita', methods=['GET'])
@require_agent_key
def agent_chart_priorita():
    """Distribuzione per priorita."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.priority, COUNT(*) as totale
                FROM bookings b
                JOIN triage_results t ON b.triage_id = t.id
                GROUP BY t.priority ORDER BY totale DESC
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/chart/categorie', methods=['GET'])
@require_agent_key
def agent_chart_categorie():
    """Distribuzione per categoria."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, COUNT(*) as totale
                FROM triage_results
                GROUP BY category ORDER BY totale DESC
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/chart/conversazioni', methods=['GET'])
@require_agent_key
def agent_chart_conversazioni():
    """Conversazioni per giorno (ultimo mese)."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT created_at::date::text as giorno, COUNT(*) as totale
                FROM conversations
                WHERE created_at >= CURRENT_DATE - interval '30 days'
                GROUP BY created_at::date ORDER BY created_at::date
            """)
            return {'data': cur.fetchall()}
    finally:
        conn.close()


@app.route('/api/agent/query', methods=['POST'])
@require_agent_key
def agent_query():
    """Query SQL di sola lettura per la chat AI."""
    data = request.get_json()
    sql = data.get('sql', '')
    if not sql.strip().upper().startswith('SELECT'):
        return {'error': 'Solo query SELECT permesse'}, 400
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return {'data': cur.fetchall()}
    except Exception as e:
        return {'error': str(e)}, 400
    finally:
        conn.close()


# Inizializza il database all'avvio
db.init_db()

# Avvia il thread promemoria
threading.Thread(target=scheduler_promemoria, daemon=True).start()
logger.info('Thread promemoria avviato')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info('\U0001f680 ' + NOME + ' avviato sulla porta ' + str(port))
    app.run(host='0.0.0.0', port=port)
