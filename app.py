import os
import json
import re
import hmac
import hashlib
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request
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

ORARIO_APERTURA = os.environ.get('ORARIO_APERTURA', '08:30')
ORARIO_CHIUSURA = os.environ.get('ORARIO_CHIUSURA', '17:00')
PAUSA_INIZIO = os.environ.get('PAUSA_PRANZO_INIZIO', '13:00')
PAUSA_FINE = os.environ.get('PAUSA_PRANZO_FINE', '14:00')
SABATO_CHIUSURA = os.environ.get('SABATO_CHIUSURA', '13:00')
DURATA_SLOT = int(os.environ.get('DURATA_SLOT_MINUTI', '60'))
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
    descrizione = 'Cliente: ' + nome_cliente + '\n'
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

def transcribe_audio(media_id):
    """Scarica un vocale da Meta e lo trascrive con Whisper."""
    try:
        # 1. Ottieni URL del media da Meta
        media_resp = req.get(
            'https://graph.facebook.com/v21.0/' + media_id,
            headers={'Authorization': 'Bearer ' + META_ACCESS_TOKEN},
            timeout=10
        )
        media_url = media_resp.json().get('url')
        if not media_url:
            logger.error('Trascrizione: URL media non trovato per ' + media_id)
            return None

        # 2. Scarica il file audio
        audio_resp = req.get(
            media_url,
            headers={'Authorization': 'Bearer ' + META_ACCESS_TOKEN},
            timeout=30
        )
        if not audio_resp.ok:
            logger.error('Trascrizione: download audio fallito - ' + str(audio_resp.status_code))
            return None

        # 3. Salva in file temporaneo e trascrivi con Whisper
        with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
            tmp.write(audio_resp.content)
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

# --- SYSTEM PROMPT ---
SYSTEM_PROMPT = '''Sei l'assistente WhatsApp dell'officina ''' + NOME + ''' (''' + INDIRIZZO + ''', tel. ''' + TELEFONO + ''').

COMPITO: accogliere il cliente con empatia, capire il problema auto e classificarlo per fissare un appuntamento.

SUPPORTO EMOTIVO (PRIORITA' MASSIMA):
- Prima di tutto, valuta lo stato emotivo del cliente dal tono del messaggio.
- Se il cliente mostra segni di ansia, panico o paura (es. "sono fermo", "ho paura", "non so cosa fare", "aiuto", "sono in panico", "e' grave?", uso di punti esclamativi, tono concitato):
  1. PRIMA rassicuralo con empatia: "Non si preoccupi, siamo qui per aiutarla." / "Capisco la sua preoccupazione, vediamo insieme."
  2. Se e' fermo in strada o in situazione di pericolo, ricordagli di mettersi in sicurezza.
  3. POI prosegui con le domande tecniche.
- Se il cliente e' tranquillo, procedi normalmente senza rassicurazioni non necessarie.
- NON minimizzare mai il problema ("ma non e' niente"). Mostra comprensione.
- NON esagerare con l'allarmismo. Sii equilibrato e professionale.

REGOLE DI COMUNICAZIONE:
- Rispondi SEMPRE in 1-3 frasi brevi. MAI piu' di 4 frasi.
- NON salutare se il cliente ha gia' descritto un problema. Vai dritto al supporto o alla domanda.
- Saluta SOLO se il cliente scrive "ciao" o un saluto generico senza descrivere problemi.
- Fai UNA domanda alla volta.
- Dai del "Lei".
- NON ripetere quello che il cliente ha detto.
- Usa un tono caldo e professionale, come un meccanico di fiducia.
- NON suggerire MAI al cliente di chiamare l'officina o di farsi richiamare. Tu gestisci TUTTO: prenotazioni, spostamenti, cancellazioni. Se il cliente chiede di spostare o cancellare un appuntamento, digli che puo' farlo direttamente qui in chat.

DOPO 2-3 SCAMBI, rispondi SOLO con questo JSON (niente altro testo):
{"triage_complete":true,"priority":"CRITICA|ALTA|MEDIA|BASSA","category":"motore|trasmissione|freni|sterzo|sospensioni|impianto_elettrico|climatizzazione|carrozzeria|pneumatici|luci|tergicristalli|batteria|scarico|tagliando|altro","summary":"Breve descrizione","recommendation":"Cosa consigliamo","emotional_note":"Stato emotivo del cliente: calmo|preoccupato|ansioso|in_panico"}

PRIORITA':
- CRITICA: veicolo non guidabile, sicurezza compromessa
- ALTA: problema serio ma utilizzabile con cautela
- MEDIA: da risolvere ma non urgente
- BASSA: manutenzione ordinaria o estetica

NON classificare al primo messaggio. Fai ALMENO 1 domanda prima.'''

# --- SLOT E PRIORITA' ---
PRIORITY_CONFIG = {
    'CRITICA': {'emoji': '\U0001f534', 'label': 'URGENTE', 'min_days': 0, 'max_days': 2, 'slots': 4},
    'ALTA':    {'emoji': '\U0001f7e0', 'label': 'PRIORITARIO', 'min_days': 0, 'max_days': 4, 'slots': 3},
    'MEDIA':   {'emoji': '\U0001f7e1', 'label': 'NORMALE', 'min_days': 0, 'max_days': 7, 'slots': 3},
    'BASSA':   {'emoji': '\U0001f7e2', 'label': 'BASSA', 'min_days': 15, 'max_days': 30, 'slots': 3},
}

GIORNI = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom']
MESI = ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic']
GIORNI_LAVORATIVI = [0, 1, 2, 3, 4, 5]

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

def genera_slot(priority):
    config = PRIORITY_CONFIG[priority]
    min_days = config.get('min_days', 0)
    max_days = config['max_days']
    now = datetime.now(TZ_ROME)
    time_min = (now + timedelta(days=min_days)).isoformat()
    time_max = (now + timedelta(days=max_days + 1)).isoformat()
    busy_times = get_busy_times(time_min, time_max)
    slots = []
    for delta in range(min_days, max_days + 1):
        date = now + timedelta(days=delta)
        if date.weekday() not in GIORNI_LAVORATIVI:
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
    n = config['slots']
    if len(slots) <= n:
        return slots
    step = len(slots) // n
    return [slots[i * step] for i in range(n)]

CATEGORY_LABELS = {
    'motore': 'Motore', 'trasmissione': 'Trasmissione', 'freni': 'Freni',
    'sterzo': 'Sterzo', 'sospensioni': 'Sospensioni',
    'impianto_elettrico': 'Impianto Elettrico', 'climatizzazione': 'Climatizzazione',
    'carrozzeria': 'Carrozzeria', 'pneumatici': 'Pneumatici', 'luci': 'Luci',
    'tergicristalli': 'Tergicristalli', 'batteria': 'Batteria', 'scarico': 'Scarico',
    'tagliando': 'Tagliando', 'altro': 'Altro',
}

def formatta_triage(triage):
    pri = triage['priority']
    config = PRIORITY_CONFIG[pri]
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
    msg += '\U0001f4c5 *Slot disponibili:*\n'
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
    msg = '\u2705 *Appuntamento Confermato!*\n\n'
    msg += '\U0001f464 *Cliente:* ' + nome + '\n'
    msg += '\U0001f697 *Veicolo:* ' + auto + '\n'
    msg += '\U0001f4c5 ' + slot['display'] + '\n'
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

WELCOME_MSG = ('Benvenuto nell\'assistenza *' + NOME + '*! \U0001f697\n'
    'Sono il suo assistente digitale, disponibile 24 ore su 24.\n'
    'Come posso aiutarla? Scelga un\'opzione o mi descriva il suo problema:\n\n'
    '1\ufe0f\u20e3 Ho un problema con la mia auto\n'
    '2\ufe0f\u20e3 Vorrei prenotare un tagliando\n'
    '3\ufe0f\u20e3 Vorrei spostare o cancellare un appuntamento\n'
    '4\ufe0f\u20e3 Informazioni e orari officina\n\n'
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

def process_message(from_number, incoming_msg):
    logger.info('\U0001f4e9 ' + from_number + ': ' + incoming_msg)
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
        if msg_stripped in ['1', '2', '3', '4']:
            claude_msgs = [{'role': 'user', 'content': incoming_msg}, {'role': 'assistant', 'content': WELCOME_MSG}]
            db.save_conversation(from_number, claude_msgs, None)
            send_whatsapp_message(from_number, WELCOME_MSG)
            if msg_stripped == '3':
                return avvia_flusso_modifica(from_number, claude_msgs)
            if msg_stripped == '4':
                return INFO_MSG
            if msg_stripped == '2':
                incoming_msg = 'Vorrei prenotare un tagliando'
            else:
                incoming_msg = 'Ho un problema con la mia auto'
            messages = claude_msgs
            # Prosegui al flusso Claude sotto
        else:
            claude_msgs = [{'role': 'user', 'content': incoming_msg}, {'role': 'assistant', 'content': WELCOME_MSG}]
            db.save_conversation(from_number, claude_msgs, None)
            return WELCOME_MSG
    # Gestione risposte rapide dal menu
    msg_stripped = incoming_msg.strip()
    if not pending_slots and msg_stripped in ['1', '2']:
        # Opzione 1 o 2: avvia triage (per il tagliando, simula un messaggio iniziale)
        if msg_stripped == '2':
            incoming_msg = 'Vorrei prenotare un tagliando'
        else:
            incoming_msg = 'Ho un problema con la mia auto'
        # Lascia proseguire al flusso Claude normale sotto
    elif not pending_slots and msg_stripped == '3':
        return avvia_flusso_modifica(from_number, messages)
    elif not pending_slots and msg_stripped == '4':
        return INFO_MSG
    # Rileva intent spostamento/cancellazione (solo se non c'è già un flusso in corso)
    if not pending_slots and is_reschedule_intent(incoming_msg):
        return avvia_flusso_modifica(from_number, messages)
    if pending_slots:
        result = gestisci_prenotazione(from_number, incoming_msg, pending_slots)
        if result:
            return result
    claude_msgs = [m for m in messages if isinstance(m, dict) and 'role' in m]
    claude_msgs.append({'role': 'user', 'content': incoming_msg})
    try:
        response = claude_client.messages.create(
            model='claude-sonnet-4-6', max_tokens=1000,
            system=SYSTEM_PROMPT, messages=claude_msgs,
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

        if msg_type == 'text':
            text = msg.get('text', {}).get('body', '').strip()
        elif msg_type == 'audio' and openai_client:
            audio_id = msg.get('audio', {}).get('id', '')
            if audio_id:
                text = transcribe_audio(audio_id)
                if not text:
                    send_whatsapp_message(phone, 'Non sono riuscito a capire il vocale. Puoi ripetere o scrivere?')

        if text:
            def process_and_send():
                lock = get_user_lock(phone)
                with lock:
                    reply = process_message(phone, text)
                    send_whatsapp_message(phone, reply)
            threading.Thread(target=process_and_send, daemon=True).start()
        elif msg_type not in ('text', 'audio'):
            send_whatsapp_message(phone, 'Posso elaborare solo messaggi di testo e vocali.')
    except Exception as e:
        logger.error('Webhook errore: ' + str(e))
    return 'OK', 200

# Inizializza il database all'avvio
db.init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info('\U0001f680 ' + NOME + ' avviato sulla porta ' + str(port))
    app.run(host='0.0.0.0', port=port)
