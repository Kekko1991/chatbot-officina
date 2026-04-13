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
from apscheduler.schedulers.background import BackgroundScheduler

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
TEMPLATE_NAME = os.environ.get('TEMPLATE_NAME', 'ritiro_veicolo')
TEMPLATE_PROMEMORIA = os.environ.get('TEMPLATE_PROMEMORIA', 'promemoria_consegna')
TEMPLATE_NON_PRONTA = os.environ.get('TEMPLATE_NON_PRONTA', 'lavorazione_veicolo')
TEMPLATE_PROMEMORIA_GIORNO = os.environ.get('TEMPLATE_PROMEMORIA_GIORNO', 'promemoria_consegna_oggi')

LATITUDINE = os.environ.get('LATITUDINE_OFFICINA', '40.8518')
LONGITUDINE = os.environ.get('LONGITUDINE_OFFICINA', '14.2681')

ORARIO_APERTURA = os.environ.get('ORARIO_APERTURA', '08:30')
ORARIO_CHIUSURA = os.environ.get('ORARIO_CHIUSURA', '17:00')
PAUSA_INIZIO = os.environ.get('PAUSA_PRANZO_INIZIO', '13:00')
PAUSA_FINE = os.environ.get('PAUSA_PRANZO_FINE', '14:00')
SABATO_CHIUSURA = os.environ.get('SABATO_CHIUSURA', '13:00')
DURATA_SLOT = int(os.environ.get('DURATA_SLOT_MINUTI', '60'))
SPAZIATURA_MINUTI = 90  # 1.5 ore tra appuntamenti
RANGE_MATTINA = ('09:00', '12:00')
RANGE_POMERIGGIO = ('15:00', '18:30')
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
    cov = triage_data.get('coverage_type', '')
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
    if cov:
        cov_config = COVERAGE_LABELS.get(cov, {})
        descrizione += 'Copertura: ' + cov_config.get('label', cov) + '\n'
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
        else:
            logger.info('✅ Risposta inviata a ' + to_number + ' (' + str(len(text)) + ' car.)')
        return r.status_code == 200
    except Exception as e:
        logger.error('Invio errore: ' + str(e))
        return False

def send_whatsapp_location(to_number):
    """Invia la posizione dell'officina su WhatsApp."""
    payload = {
        'messaging_product': 'whatsapp',
        'to': to_number,
        'type': 'location',
        'location': {
            'latitude': LATITUDINE,
            'longitude': LONGITUDINE,
            'name': NOME,
            'address': INDIRIZZO
        }
    }
    try:
        r = req.post(META_API_URL, headers=META_HEADERS, json=payload)
        if r.status_code != 200:
            logger.error('Invio posizione errore: ' + str(r.status_code) + ' ' + r.text)
        return r.status_code == 200
    except Exception as e:
        logger.error('Invio posizione errore: ' + str(e))
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
SYSTEM_PROMPT = '''Sei l'assistente WhatsApp del concessionario ''' + NOME + ''' (''' + INDIRIZZO + ''', tel. ''' + TELEFONO + ''').

COMPITO: capire il problema dell'auto, determinare se rientra nel Check Qualita' Veicolo o nella garanzia ordinaria, applicare la regola dei 10 giorni e, se consentito, fissare un appuntamento.

REGOLA FONDAMENTALE: NON menzionare MAI al cliente la regola dei 10 giorni, il termine "10 giorni", o il fatto che esista un limite di tempo per il Check Qualita'. Questa e' un'informazione INTERNA che il cliente NON deve conoscere. Usa la regola internamente per decidere se procedere o rifiutare, ma non citarla nelle risposte.

STILE DI COMUNICAZIONE:
- Sii diretto, professionale e schematico. Niente giri di parole.
- NON essere apprensivo o eccessivamente empatico.
- Rispondi in 1-2 frasi brevi. MAI piu' di 3 frasi.
- Fai UNA domanda alla volta.
- Dai del "Lei".
- NON ripetere quello che il cliente ha detto.
- NON suggerire MAI al cliente di chiamare l'officina. Tu gestisci TUTTO.

RACCOLTA DETTAGLI (IMPORTANTE):
- Fai domande specifiche e tecniche per capire bene il problema.
- Chiedi SEMPRE la *data di consegna o acquisto* del veicolo.
- Chiedi da quanto tempo c'e' il problema e in che condizioni si presenta.
- Se il cliente e' vago, insisti per avere dettagli.
- Raccogli abbastanza informazioni per dare all'officina un quadro chiaro.
- NON chiedere MAI: targa, chilometraggio, tipo di alimentazione (benzina/diesel), nome e cognome. Questi dati vengono raccolti DOPO dal sistema automatico.

CHECK QUALITA' VEICOLO PRE-CONSEGNA:
Tutte le seguenti voci sono controlli effettuati PRIMA della consegna. Sono coperti SOLO entro 10 giorni dalla data di consegna:
- MANUTENZIONE: filtro olio, filtro aria, filtro antipolline, filtro carburante, kit distribuzione, pompa acqua, pastiglie freni ant/post, dischi freni ant/post, ammortizzatori, candele/candelette, batteria, cinghia servizi, liquido freni, liquido raffreddamento
- MOTORE E TRASMISSIONE (voci check): avviamento, regime minimo, fumosita' scarico, perdite olio/liquido, livelli olio/liquido, cambio marce, frizione, vibrazioni trasmissione
- IMPIANTO FRENANTE (voci check): spessore pastiglie/dischi, corsa pedale, freno stazionamento, vibrazioni frenata, livello liquido freni
- SOSPENSIONI E STERZO (voci check): ammortizzatori, giochi sospensioni, sterzo centrato, rumori sterzata, giunti e cuffie
- PNEUMATICI E RUOTE: battistrada min 3mm, usura, pressione, TPMS, cerchi, ruota scorta
- IMPIANTO ELETTRICO E ILLUMINAZIONE: batteria, fari, luci posizione, frecce, stop, retromarcia, fendinebbia, tergilunotto
- ELETTRONICA E INFOTAINMENT: spie cruscotto, quadro strumenti, climatizzatore, infotainment, radio, Bluetooth, sensori parcheggio, telecamera, cruise control
- CARROZZERIA E VETRI: ammaccature, verniciatura, parabrezza, vetri, tergicristalli, specchietti, serrature, guarnizioni
- INTERNI E SICUREZZA: sedili, cinture, tappetini, rivestimenti, maniglie, alzacristalli, portaoggetti, chiave/telecomando
- PROVA SU STRADA (voci check): tenuta strada, frenata, rumori marcia, cambio sotto carico, comfort, stabilita' curva

REGOLA DEI 10 GIORNI (OBBLIGATORIA):
1. Chiedi SEMPRE la data di consegna del veicolo.
2. Determina se il problema rientra nel Check Qualita' Veicolo (voci sopra) o nella garanzia ordinaria.
3. Se e' Check Qualita' e NON sono passati 10 giorni dalla consegna: puoi procedere con l'appuntamento.
4. Se e' Check Qualita' e SONO passati piu' di 10 giorni dalla consegna: RIFIUTA FERMAMENTE. Rispondi:
   "La problematica da lei segnalata rientra nel Check Qualita' Veicolo, il documento che le e' stato consegnato al momento della consegna dell'auto. Per queste voci il periodo di assistenza e' purtroppo terminato. Non e' possibile fissare un appuntamento per questa tipologia di richiesta. La garanzia copre problemi relativi al motore, cambio, organi meccanici principali e difetti non legati all'usura."
   NON fare eccezioni. NON fissare appuntamento nemmeno a pagamento. NON produrre JSON triage. NON menzionare i 10 giorni.

GARANZIA ORDINARIA:
La garanzia COPRE: motore, cambio, organi meccanici principali (sterzo, cremagliera, pompa servosterzo, differenziale, semiassi, sospensioni strutturali, impianto frenante idraulico), difetti reali non legati all'usura.
La garanzia NON COPRE: parti soggette ad usura, componenti estetici o di comfort, danni causati da uso improprio o trascuratezza.
Se il problema rientra nelle categorie coperte, puoi procedere con l'appuntamento indipendentemente dalla data di consegna.
Se il problema NON e' coperto dalla garanzia (usura, estetica, uso improprio), informare il cliente che non e' possibile intervenire in garanzia.

IMPORTANTE - DISTINZIONE TRA CHECK QUALITA' E GARANZIA PER LO STESSO COMPONENTE:
Alcuni componenti compaiono sia nel Check Qualita' che nella garanzia ordinaria. Devi distinguere in base alla GRAVITA':
- Sterzo non centrato, rumorino lieve, gioco minore → Check Qualita' → verifica periodo
- Sterzo duro, bloccato, perdita servosterzo, guasto cremagliera → Garanzia ordinaria → sempre coperto
- Freni che vibrano leggermente, corsa pedale lunga → Check Qualita' → verifica periodo
- Impianto frenante che non funziona, perdita liquido freni → Garanzia ordinaria → sempre coperto
- Rumore sospensioni, ammortizzatore debole → Check Qualita' → verifica periodo
- Rottura sospensione, cedimento strutturale → Garanzia ordinaria → sempre coperto
In generale: se il problema e' un DIFETTO o GUASTO meccanico importante → garanzia ordinaria. Se e' una regolazione, usura, rumore lieve o controllo → check qualita'.

QUANDO PUOI PROCEDERE, dopo 2-4 scambi rispondi SOLO con questo JSON (niente altro testo):
{"triage_complete":true,"coverage_type":"check_qualita|garanzia_ordinaria","category":"motore|trasmissione|freni|sterzo|sospensioni|impianto_elettrico|climatizzazione|carrozzeria|pneumatici|luci|tergicristalli|batteria|scarico|altro","summary":"Descrizione dettagliata del problema con tutti i dettagli raccolti","recommendation":"Cosa consigliamo","delivery_date":"YYYY-MM-DD se il cliente l'ha fornita, altrimenti null","preferred_datetime":"SOLO se il cliente ha indicato data/ora preferita, formato YYYY-MM-DD HH:MM. Altrimenti null."}

RICOVERO AUTO:
- L'officina accetta auto SOLO dal lunedi' al mercoledi' (consegna).
- L'auto resta in officina per un ricovero di 24-72 ore.
- Massimo 3 appuntamenti a settimana.
- Spiega al cliente che l'auto sara' in ricovero e che verra' contattato quando e' pronta.
- NON proporre tu un giorno o un orario specifico al cliente. Sara' il sistema a mostrare le fasce orarie disponibili dopo il triage.
- Se il cliente chiede "quando posso portarla?" o simili, rispondi che dopo la valutazione gli verranno mostrate le disponibilita'.
- Compila preferred_datetime nel JSON SOLO se il cliente ha espressamente indicato un giorno/ora precisi di sua iniziativa (es. "posso portarla lunedi' alle 10").

FOTO:
- Se il problema e' VISIVO (spia accesa, graffio, ammaccatura, danno, pneumatico, perdita liquido), chiedi una foto.
- Se e' INTERNO/MECCANICO (rumore, vibrazione, ecc.), NON chiedere foto.
- La foto NON e' mai obbligatoria.

NON classificare al primo messaggio. Fai ALMENO 2 domande prima di classificare.

SPOSTAMENTO/CANCELLAZIONE APPUNTAMENTI:
- Se il cliente chiede di spostare, modificare, cancellare o disdire un appuntamento, rispondi SOLO: "Per gestire il suo appuntamento, scelga l'opzione 2 dal menu oppure scriva 'spostare appuntamento'."
- NON gestire tu lo spostamento o la cancellazione. NON confermare date o orari per spostamenti. NON dire "prendo nota" o "il sistema aggiornera'".
- Il sistema ha un flusso dedicato per le modifiche. Tu NON puoi modificare appuntamenti.'''

# --- SLOT E PRENOTAZIONI ---
DEFAULT_BOOKING_CONFIG = {'min_days': 0, 'max_days': 21}

COVERAGE_LABELS = {
    'check_qualita': {'emoji': '\U0001f6e1\ufe0f', 'label': 'Check Qualita\' Veicolo'},
    'garanzia_ordinaria': {'emoji': '\U0001f527', 'label': 'Garanzia Ordinaria'},
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

# --- INTERPRETAZIONE TESTO LIBERO PER SLOT ---
MESI_FULL = {
    'gennaio': 1, 'febbraio': 2, 'marzo': 3, 'aprile': 4,
    'maggio': 5, 'giugno': 6, 'luglio': 7, 'agosto': 8,
    'settembre': 9, 'ottobre': 10, 'novembre': 11, 'dicembre': 12,
}
GIORNI_KEYWORDS = {
    'lunedi': 0, 'lunedì': 0,
    'martedi': 1, 'martedì': 1,
    'mercoledi': 2, 'mercoledì': 2,
    'giovedi': 3, 'giovedì': 3,
    'venerdi': 4, 'venerdì': 4,
    'sabato': 5, 'domenica': 6,
}

def _rileva_intento_modifica(msg):
    """Usa Haiku per capire se il cliente vuole spostare/modificare/cancellare un appuntamento esistente.
    Ritorna True se vuole modificare, False altrimenti."""
    try:
        response = claude_client.messages.create(
            model='claude-haiku-4-5-20251001', max_tokens=10,
            system='Sei un classificatore di intenti. Rispondi SOLO con "MODIFICA" o "ALTRO".',
            messages=[{'role': 'user', 'content':
                'Il cliente ha scritto: "' + msg + '"\n\n'
                'Il cliente vuole spostare, modificare, cancellare o disdire un appuntamento esistente (MODIFICA)? '
                'Oppure sta parlando di altro, come descrivere un problema alla sua auto o chiedere informazioni (ALTRO)?'
            }],
        )
        return 'MODIFICA' in response.content[0].text.upper()
    except Exception as e:
        logger.error('Errore intent modifica: ' + str(e))
        return False


def _rileva_intento_cambio(msg, contesto):
    """Usa Claude (haiku, veloce e economico) per capire se il cliente vuole cambiare orario
    invece di fornire il dato richiesto (nome o veicolo).
    Ritorna True se il cliente vuole cambiare orario, False se sta fornendo il dato richiesto."""
    try:
        response = claude_client.messages.create(
            model='claude-haiku-4-5-20251001', max_tokens=10,
            system='Sei un classificatore di intenti. Rispondi SOLO con "CAMBIO" o "DATO".',
            messages=[{'role': 'user', 'content':
                'Il chatbot ha chiesto al cliente: ' + contesto + '\n'
                'Il cliente ha risposto: "' + msg + '"\n\n'
                'Il cliente sta fornendo il dato richiesto (DATO) oppure vuole cambiare/rifiutare l\'orario dell\'appuntamento (CAMBIO)?'
            }],
        )
        return 'CAMBIO' in response.content[0].text.upper()
    except Exception as e:
        logger.error('Errore intent detection: ' + str(e))
        return False


def _interpreta_data_libera(msg):
    """Estrae data, orario e fascia dal testo libero del cliente.
    Ritorna (preferred_date, preferred_time, preferred_period) dove:
    - preferred_date: datetime con timezone o None
    - preferred_time: stringa 'HH:MM' o None
    - preferred_period: 'mattina' o 'pomeriggio' o None
    """
    msg_lower = msg.strip().lower()
    now = datetime.now(TZ_ROME)
    preferred_date = None
    preferred_time = None
    preferred_period = None

    # Estrai orario specifico: "alle 10", "ore 15:30", "alle ore 9"
    time_match = re.search(r'(?:alle|ore)\s*(\d{1,2})(?::(\d{2}))?', msg_lower)
    if time_match:
        h = int(time_match.group(1))
        m = int(time_match.group(2)) if time_match.group(2) else 0
        if 0 <= h <= 23 and 0 <= m <= 59:
            preferred_time = str(h).zfill(2) + ':' + str(m).zfill(2)

    # Estrai fascia oraria
    if any(w in msg_lower for w in ['mattina', 'mattino']):
        preferred_period = 'mattina'
    elif any(w in msg_lower for w in ['pomeriggio', 'dopo pranzo']):
        preferred_period = 'pomeriggio'
    elif preferred_time:
        preferred_period = 'mattina' if preferred_time < '13:00' else 'pomeriggio'

    # Estrai data: "domani", "dopodomani"
    if 'domani' in msg_lower and 'dopodomani' not in msg_lower:
        preferred_date = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif 'dopodomani' in msg_lower:
        preferred_date = (now + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Estrai data: "20 aprile", "il 15 maggio"
    if not preferred_date:
        for mese_name, mese_num in MESI_FULL.items():
            date_match = re.search(r'(\d{1,2})\s+(?:di\s+)?' + mese_name, msg_lower)
            if date_match:
                day = int(date_match.group(1))
                year = now.year
                try:
                    preferred_date = datetime(year, mese_num, day, tzinfo=TZ_ROME)
                    if preferred_date.date() < now.date():
                        preferred_date = datetime(year + 1, mese_num, day, tzinfo=TZ_ROME)
                except ValueError:
                    preferred_date = None
                break

    # Estrai data: "20/4", "15-05"
    if not preferred_date:
        date_match = re.search(r'(\d{1,2})[/\-](\d{1,2})', msg_lower)
        if date_match:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            if 1 <= day <= 31 and 1 <= month <= 12:
                year = now.year
                try:
                    preferred_date = datetime(year, month, day, tzinfo=TZ_ROME)
                    if preferred_date.date() < now.date():
                        preferred_date = datetime(year + 1, month, day, tzinfo=TZ_ROME)
                except ValueError:
                    preferred_date = None

    # Estrai giorno della settimana: "lunedì", "martedì prossimo"
    if not preferred_date:
        for nome_giorno, weekday_num in GIORNI_KEYWORDS.items():
            if nome_giorno in msg_lower:
                # Trova il prossimo giorno con quel weekday
                days_ahead = (weekday_num - now.weekday()) % 7
                if days_ahead == 0:
                    days_ahead = 7  # Se oggi è quel giorno, prendi il prossimo
                preferred_date = (now + timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
                break

    # Se niente giorno specifico, cerca almeno un numero di giorno (es. "il 20")
    if not preferred_date:
        day_match = re.search(r'(?:il|del|per il|giorno)\s+(\d{1,2})\b', msg_lower)
        if day_match:
            day = int(day_match.group(1))
            if 1 <= day <= 31:
                month = now.month
                year = now.year
                try:
                    preferred_date = datetime(year, month, day, tzinfo=TZ_ROME)
                    if preferred_date.date() < now.date():
                        month += 1
                        if month > 12:
                            month = 1
                            year += 1
                        preferred_date = datetime(year, month, day, tzinfo=TZ_ROME)
                except ValueError:
                    preferred_date = None

    return preferred_date, preferred_time, preferred_period


def _cerca_slot_per_data(preferred_date, preferred_time, preferred_period):
    """Cerca slot disponibili per una data specifica. Ritorna (slot_singolo, opzioni_lista, errore_msg)."""
    config = DEFAULT_BOOKING_CONFIG
    now = datetime.now(TZ_ROME)
    min_date = (now + timedelta(days=config.get('min_days', 0))).date()
    max_date = (now + timedelta(days=config['max_days'])).date()

    if preferred_date.weekday() not in GIORNI_LAVORATIVI:
        return None, None, '\u26a0\ufe0f L\'officina riceve solo dal *lunedi\' al mercoledi\'*.\n\nScelga un giorno tra quelli proposti oppure ne indichi un altro (Lun/Mar/Mer).'
    if preferred_date.date() < min_date:
        return None, None, '\u26a0\ufe0f La data indicata e\' troppo vicina o gia\' passata. Scelga tra le opzioni proposte.'
    if preferred_date.date() > max_date:
        return None, None, '\u26a0\ufe0f La data indicata e\' troppo lontana (max ' + str(config['max_days']) + ' giorni). Scelga tra le opzioni proposte.'

    # Controlla limite settimanale
    date_str = preferred_date.strftime('%Y-%m-%d')
    weekly_counts = db.count_bookings_by_week(date_str, date_str)
    week_start = (preferred_date - timedelta(days=preferred_date.weekday())).strftime('%Y-%m-%d')
    if weekly_counts.get(week_start, 0) >= MAX_APPUNTAMENTI_SETTIMANA:
        return None, None, '\u26a0\ufe0f Questa settimana e\' gia\' al completo (max ' + str(MAX_APPUNTAMENTI_SETTIMANA) + ' appuntamenti). Scelga un\'altra settimana.'

    time_min = datetime(preferred_date.year, preferred_date.month, preferred_date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
    time_max = datetime(preferred_date.year, preferred_date.month, preferred_date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
    busy_times = get_busy_times(time_min, time_max)

    # Se il cliente ha indicato un orario specifico, cerca quello
    if preferred_time:
        if preferred_time < '12:00':
            range_fine = RANGE_MATTINA[1]
        else:
            range_fine = RANGE_POMERIGGIO[1]
        slot = trova_slot_in_range(preferred_date, preferred_time, range_fine, busy_times)
        if slot:
            return slot, None, None
        return None, None, None  # Orario non disponibile, mostrera' le fasce

    # Se il cliente ha indicato solo mattina/pomeriggio, mostra tutti gli orari nella fascia
    if preferred_period == 'mattina':
        tutti_slot = trova_tutti_slot_in_range(preferred_date, RANGE_MATTINA[0], RANGE_MATTINA[1], busy_times, now)
        if tutti_slot:
            if len(tutti_slot) == 1:
                return tutti_slot[0], None, None
            return None, tutti_slot, None
    elif preferred_period == 'pomeriggio':
        tutti_slot = trova_tutti_slot_in_range(preferred_date, RANGE_POMERIGGIO[0], RANGE_POMERIGGIO[1], busy_times, now)
        if tutti_slot:
            if len(tutti_slot) == 1:
                return tutti_slot[0], None, None
            return None, tutti_slot, None

    # Genera opzioni mattina/pomeriggio per il giorno richiesto
    g = GIORNI[preferred_date.weekday()]
    me = MESI[preferred_date.month - 1]
    label_giorno = g + ' ' + str(preferred_date.day) + ' ' + me
    opzioni = []
    for nome_range, r_range in [('Mattina', RANGE_MATTINA), ('Pomeriggio', RANGE_POMERIGGIO)]:
        r_inizio, r_fine = r_range
        slot = trova_slot_in_range(preferred_date, r_inizio, r_fine, busy_times, now)
        if slot:
            opzioni.append({
                'display': label_giorno + ' - ' + nome_range + ' (' + r_inizio + '-' + r_fine + ')',
                'date': preferred_date.strftime('%Y-%m-%d'),
                'range_inizio': r_inizio,
                'range_fine': r_fine,
            })
    if opzioni:
        return None, opzioni, None
    return None, None, '\u26a0\ufe0f Nessuno slot disponibile per il ' + label_giorno + '. Indichi un\'altra data (Lun/Mar/Mer).'


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
    """Verifica che lo slot non si sovrapponga e rispetti la spaziatura di 1.5h."""
    spacing = timedelta(minutes=SPAZIATURA_MINUTI)
    for busy_start, busy_end in busy_times:
        # Controlla sovrapposizione diretta
        if slot_start < busy_end and slot_end > busy_start:
            return False
        # Controlla spaziatura: almeno 1.5h tra inizi appuntamento
        if abs((slot_start - busy_start).total_seconds()) < spacing.total_seconds():
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


def genera_slot(all_slots=False):
    config = DEFAULT_BOOKING_CONFIG
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

def trova_slot_in_range(date, range_inizio, range_fine, busy_times, now=None):
    """Trova il primo slot disponibile in una fascia oraria, con spaziatura di 1.5h tra appuntamenti."""
    slots = trova_tutti_slot_in_range(date, range_inizio, range_fine, busy_times, now)
    return slots[0] if slots else None

def trova_tutti_slot_in_range(date, range_inizio, range_fine, busy_times, now=None):
    """Trova TUTTI gli slot disponibili in una fascia oraria, con spaziatura di 1.5h tra appuntamenti."""
    if now is None:
        now = datetime.now(TZ_ROME)
    ri_h, ri_m = map(int, range_inizio.split(':'))
    rf_h, rf_m = map(int, range_fine.split(':'))
    range_end = datetime(date.year, date.month, date.day, rf_h, rf_m, tzinfo=TZ_ROME)
    cur_h, cur_m = ri_h, ri_m
    risultati = []
    while True:
        candidate_start = datetime(date.year, date.month, date.day, cur_h, cur_m, tzinfo=TZ_ROME)
        candidate_end = candidate_start + timedelta(minutes=DURATA_SLOT)
        if candidate_end > range_end:
            break
        if candidate_start <= now:
            cur_m += SPAZIATURA_MINUTI
            cur_h += cur_m // 60
            cur_m = cur_m % 60
            continue
        is_free = True
        skip_to = None
        for busy_start, busy_end in busy_times:
            if candidate_start < busy_end and candidate_end > busy_start:
                is_free = False
                # Prossimo tentativo: 1.5h dopo l'inizio dell'appuntamento occupato
                skip_to = busy_start + timedelta(minutes=SPAZIATURA_MINUTI)
                break
        if is_free:
            orario = str(cur_h).zfill(2) + ':' + str(cur_m).zfill(2)
            g = GIORNI[date.weekday()]
            me = MESI[date.month - 1]
            risultati.append({
                'display': g + ' ' + str(date.day) + ' ' + me + ' ore ' + orario,
                'date': date.strftime('%Y-%m-%d'), 'time': orario,
                'datetime_start': candidate_start.isoformat(),
                'datetime_end': candidate_end.isoformat(),
            })
        if skip_to and skip_to > candidate_start:
            cur_h, cur_m = skip_to.hour, skip_to.minute
        else:
            cur_m += SPAZIATURA_MINUTI
            cur_h += cur_m // 60
            cur_m = cur_m % 60
    return risultati


def genera_opzioni_range():
    """Genera opzioni giorno + fascia oraria della prossima settimana disponibile (Lun-Mer)."""
    config = DEFAULT_BOOKING_CONFIG
    min_days = config.get('min_days', 0)
    max_days = config['max_days']
    now = datetime.now(TZ_ROME)
    date_start = (now + timedelta(days=min_days)).strftime('%Y-%m-%d')
    date_end = (now + timedelta(days=max_days)).strftime('%Y-%m-%d')
    time_min = (now + timedelta(days=min_days)).isoformat()
    time_max = (now + timedelta(days=max_days + 1)).isoformat()
    busy_times = get_busy_times(time_min, time_max)
    weekly_counts = db.count_bookings_by_week(date_start, date_end)
    # Cerca la prima settimana con disponibilita'
    for delta in range(min_days, max_days + 1):
        date = now + timedelta(days=delta)
        if date.weekday() not in GIORNI_LAVORATIVI:
            continue
        week_start = (date - timedelta(days=date.weekday())).strftime('%Y-%m-%d')
        if weekly_counts.get(week_start, 0) >= MAX_APPUNTAMENTI_SETTIMANA:
            continue
        # Trovata una settimana valida: genera tutte le opzioni di questa settimana
        opzioni = []
        for d in range(3):  # Lun(0), Mar(1), Mer(2)
            week_date = date - timedelta(days=date.weekday()) + timedelta(days=d)
            if week_date < now.replace(hour=0, minute=0, second=0, microsecond=0):
                continue
            if week_date.weekday() not in GIORNI_LAVORATIVI:
                continue
            g = GIORNI[week_date.weekday()]
            me = MESI[week_date.month - 1]
            label_giorno = g + ' ' + str(week_date.day) + ' ' + me
            for nome_range, r_range in [('Mattina', RANGE_MATTINA), ('Pomeriggio', RANGE_POMERIGGIO)]:
                r_inizio, r_fine = r_range
                slot = trova_slot_in_range(week_date, r_inizio, r_fine, busy_times, now)
                if slot:
                    opzioni.append({
                        'display': label_giorno + ' - ' + nome_range + ' (' + r_inizio + '-' + r_fine + ')',
                        'date': week_date.strftime('%Y-%m-%d'),
                        'range_inizio': r_inizio,
                        'range_fine': r_fine,
                    })
        if opzioni:
            return opzioni
    return []


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
    cov = triage.get('coverage_type', 'garanzia_ordinaria')
    cov_config = COVERAGE_LABELS.get(cov, COVERAGE_LABELS['garanzia_ordinaria'])
    preferred = triage.get('preferred_datetime')
    config = DEFAULT_BOOKING_CONFIG
    msg = cov_config['emoji'] + ' *' + cov_config['label'] + '*\n\n'
    msg += '\U0001f4cb *Problema:* ' + triage['summary'] + '\n\n'
    # Se il cliente ha indicato un orario preferito, cerca il primo slot disponibile da quell'ora
    if preferred:
        try:
            pref = datetime.strptime(preferred, '%Y-%m-%d %H:%M')
            pref_date = pref.replace(tzinfo=TZ_ROME)
            pref_time_str = pref.strftime('%H:%M')
            now = datetime.now(TZ_ROME)
            min_date = now + timedelta(days=config.get('min_days', 0))
            max_date = now + timedelta(days=config['max_days'])
            if (pref_date.weekday() in GIORNI_LAVORATIVI and
                pref_date.date() >= min_date.date() and
                pref_date.date() <= max_date.date()):
                time_min = datetime(pref_date.year, pref_date.month, pref_date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
                time_max = datetime(pref_date.year, pref_date.month, pref_date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
                busy_times = get_busy_times(time_min, time_max)
                if pref_time_str < '12:00':
                    range_fine = RANGE_MATTINA[1]
                else:
                    range_fine = RANGE_POMERIGGIO[1]
                slot = trova_slot_in_range(pref_date, pref_time_str, range_fine, busy_times)
                if slot:
                    if slot['time'] == pref_time_str:
                        msg += '\U0001f4c5 Perfetto, abbiamo disponibilita\' per *' + slot['display'] + '*.\n\n'
                        msg += 'Confermiamo questo orario?'
                    else:
                        msg += '\U0001f4c5 Le ore ' + pref_time_str + ' non sono disponibili.\n'
                        msg += 'Il primo orario disponibile e\' *' + slot['display'] + '*.\n\n'
                        msg += 'Va bene questo orario?'
                    return msg, [slot]
        except (ValueError, TypeError):
            pass
    # Genera opzioni fascia oraria (mattina/pomeriggio)
    opzioni = genera_opzioni_range()
    if not opzioni:
        msg += '\u26a0\ufe0f Nessuno slot disponibile al momento.\n'
        msg += 'Ci contatti al ' + TELEFONO + ' per fissare un appuntamento.'
        return msg, []
    msg += '\U0001f504 L\'auto restera\' in officina per un ricovero di 24-72 ore.\n\n'
    msg += '\U0001f4c5 *Quando puo\' portare l\'auto?*\n'
    for i, opt in enumerate(opzioni, 1):
        msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
    msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
    msg += '\n\n\U0001f4ac Se preferisce un\'altra data, la indichi pure. L\'officina riceve dal *lunedi\' al mercoledi\'*, mattina 9:00-12:00 e pomeriggio 15:00-18:30.'
    return msg, opzioni

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
    msg += '\n\nSara\' contattato dall\'officina per aggiornamenti sullo stato della sua auto.\nGrazie e a presto! \U0001f44b'
    # Invia messaggio conferma, poi posizione e feedback
    send_whatsapp_message(phone, msg)
    # Invia posizione Google Maps dell'officina
    send_whatsapp_location(phone)
    feedback_msg = '\U0001f4dd L\'assistente WhatsApp le e\' stato utile? Risponda da *1* (per niente) a *5* (molto utile).'
    # Salva stato feedback nella conversazione
    feedback_pending = {'state': 'waiting_feedback', 'booking_id': booking_id}
    messages, _ = db.get_conversation(phone)
    db.save_conversation(phone, messages, feedback_pending)
    if WHATSAPP_OFFICINA:
        notifica = '\U0001f4cb *Nuova Prenotazione*\n\n'
        notifica += '\U0001f464 *Cliente:* ' + nome + '\n'
        notifica += '\U0001f697 *Veicolo:* ' + auto + '\n'
        notifica += '\U0001f4de *Telefono:* ' + phone + '\n'
        notifica += '\U0001f4c5 *Appuntamento:* ' + slot['display'] + '\n'
        cov = triage_data.get('coverage_type', '')
        if cov:
            cov_config = COVERAGE_LABELS.get(cov, {})
            notifica += '\U0001f6e1\ufe0f *Copertura:* ' + cov_config.get('label', cov) + '\n'
        if triage_data.get('category'):
            label = CATEGORY_LABELS.get(triage_data['category'], triage_data['category'])
            notifica += '\U0001f3f7\ufe0f *Categoria:* ' + label + '\n'
        if triage_data.get('summary'):
            notifica += '\U0001f4dd *Problema:* ' + triage_data['summary'] + '\n'
        if triage_data.get('recommendation'):
            notifica += '\U0001f4a1 *Consiglio:* ' + triage_data['recommendation'] + '\n'
        logger.info('Invio notifica a ' + WHATSAPP_OFFICINA)
        result = send_whatsapp_message(WHATSAPP_OFFICINA, notifica)
        logger.info('Notifica inviata: ' + str(result))
    return feedback_msg

def gestisci_prenotazione(phone, scelta, pending_data):
    # pending_data puo' essere:
    # - lista di slot (scelta slot)
    # - dict con state (raccolta dati cliente)
    if isinstance(pending_data, list):
        # Fase 1: scelta fascia oraria o slot diretto
        try:
            idx = int(scelta.strip()) - 1
            if 0 <= idx < len(pending_data):
                opzione = pending_data[idx]
                if 'range_inizio' in opzione:
                    # Fascia oraria: mostra tutti gli orari disponibili nel range
                    date = datetime.strptime(opzione['date'], '%Y-%m-%d').replace(tzinfo=TZ_ROME)
                    time_min = datetime(date.year, date.month, date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
                    time_max = datetime(date.year, date.month, date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
                    busy_times = get_busy_times(time_min, time_max)
                    tutti_slot = trova_tutti_slot_in_range(date, opzione['range_inizio'], opzione['range_fine'], busy_times)
                    if not tutti_slot:
                        return '\u26a0\ufe0f Questa fascia non e\' piu\' disponibile. Scelga un\'altra opzione.'
                    if len(tutti_slot) == 1:
                        # Un solo orario disponibile: assegna direttamente
                        slot = tutti_slot[0]
                    else:
                        # Piu' orari: mostra la lista al cliente
                        msg = '\U0001f552 *Orari disponibili:*\n'
                        for i, s in enumerate(tutti_slot, 1):
                            msg += '  *' + str(i) + '.* ' + s['display'] + '\n'
                        msg += '\n\U0001f449 Risponda con il *numero* dell\'orario preferito.'
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, tutti_slot)
                        return msg
                else:
                    slot = opzione
                new_pending = {'state': 'waiting_name', 'slot': slot}
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, new_pending)
                return '\U0001f4c5 Orario assegnato: *' + slot['display'] + '*\n\n\U0001f464 Per completare la prenotazione, mi dica *nome e cognome*.'
            else:
                return '\u26a0\ufe0f Scelta non valida. Risponda con un numero da 1 a ' + str(len(pending_data)) + '.'
        except ValueError:
            risposta = scelta.strip().lower()
            # Se il cliente conferma l'orario proposto (slot singolo)
            if risposta in ['si', 'sì', 'ok', 'va bene', 'perfetto', 'confermo', 'certo', 'assolutamente', 'conferma', 'yes', 'esatto', 'giusto']:
                if len(pending_data) == 1:
                    opzione = pending_data[0]
                    if 'range_inizio' in opzione:
                        date = datetime.strptime(opzione['date'], '%Y-%m-%d').replace(tzinfo=TZ_ROME)
                        time_min = datetime(date.year, date.month, date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
                        time_max = datetime(date.year, date.month, date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
                        busy_times = get_busy_times(time_min, time_max)
                        slot = trova_slot_in_range(date, opzione['range_inizio'], opzione['range_fine'], busy_times)
                        if not slot:
                            return '\u26a0\ufe0f Questo slot non e\' piu\' disponibile. Scelga un\'altra opzione.'
                    else:
                        slot = opzione
                    new_pending = {'state': 'waiting_name', 'slot': slot}
                    messages, _ = db.get_conversation(phone)
                    db.save_conversation(phone, messages, new_pending)
                    return '\U0001f4c5 Orario assegnato: *' + slot['display'] + '*\n\n\U0001f464 Per completare la prenotazione, mi dica *nome e cognome*.'
            # Se il cliente rifiuta l'orario proposto, mostra le fasce orarie
            if risposta in ['no', 'nono', 'non', 'no grazie', 'nah', 'negativo'] or _rileva_intento_cambio(risposta, 'confermiamo questo orario?'):
                triage = db.get_latest_triage(phone)
                opzioni = genera_opzioni_range()
                if opzioni:
                    msg = '\U0001f4c5 *Quando puo\' portare l\'auto?*\n'
                    for i, opt in enumerate(opzioni, 1):
                        msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                    msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                    messages, _ = db.get_conversation(phone)
                    db.save_conversation(phone, messages, opzioni)
                    return msg
                else:
                    messages, _ = db.get_conversation(phone)
                    db.save_conversation(phone, messages, None)
                    return '\u26a0\ufe0f Nessuno slot disponibile al momento.\nCi contatti al ' + TELEFONO + ' per fissare un appuntamento.'
            # Tenta di interpretare testo libero come preferenza data/ora
            preferred_date, preferred_time, preferred_period = _interpreta_data_libera(scelta)
            if preferred_date or preferred_time or preferred_period:
                # Prima prova a filtrare le opzioni gia' mostrate
                if preferred_date:
                    pref_date_str = preferred_date.strftime('%Y-%m-%d')
                    filtered = [opt for opt in pending_data if opt['date'] == pref_date_str]
                    if preferred_period and filtered:
                        filtered = [opt for opt in filtered if
                            (preferred_period == 'mattina' and opt.get('range_inizio', opt.get('time', '')) < '13:00') or
                            (preferred_period == 'pomeriggio' and opt.get('range_inizio', opt.get('time', '')) >= '13:00')]
                    if len(filtered) == 1:
                        # Match esatto tra le opzioni mostrate
                        opzione = filtered[0]
                        if 'range_inizio' in opzione:
                            date = datetime.strptime(opzione['date'], '%Y-%m-%d').replace(tzinfo=TZ_ROME)
                            tm = datetime(date.year, date.month, date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
                            tx = datetime(date.year, date.month, date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
                            bt = get_busy_times(tm, tx)
                            start_time = preferred_time if preferred_time else opzione['range_inizio']
                            tutti_slot = trova_tutti_slot_in_range(date, start_time, opzione['range_fine'], bt)
                            if not tutti_slot:
                                return '\u26a0\ufe0f Questa fascia non e\' piu\' disponibile. Scelga un\'altra opzione.'
                            if len(tutti_slot) == 1:
                                slot = tutti_slot[0]
                            else:
                                msg = '\U0001f552 *Orari disponibili:*\n'
                                for i, s in enumerate(tutti_slot, 1):
                                    msg += '  *' + str(i) + '.* ' + s['display'] + '\n'
                                msg += '\n\U0001f449 Risponda con il *numero* dell\'orario preferito.'
                                messages, _ = db.get_conversation(phone)
                                db.save_conversation(phone, messages, tutti_slot)
                                return msg
                        else:
                            slot = opzione
                        new_pending = {'state': 'waiting_name', 'slot': slot}
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, new_pending)
                        return '\U0001f4c5 Orario assegnato: *' + slot['display'] + '*\n\n\U0001f464 Per completare la prenotazione, mi dica *nome e cognome*.'
                    elif len(filtered) > 1:
                        msg = '\U0001f4c5 Per quel giorno abbiamo queste disponibilita\':\n'
                        for i, opt in enumerate(filtered, 1):
                            msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                        msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, filtered)
                        return msg
                # Data non presente nelle opzioni correnti: genera nuovi slot
                if preferred_date:
                    slot, opzioni, errore = _cerca_slot_per_data(preferred_date, preferred_time, preferred_period)
                    if errore:
                        return errore
                    if slot:
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, [slot])
                        return '\U0001f4c5 Disponibilita\' trovata: *' + slot['display'] + '*\n\nConfermiamo questo orario?'
                    if opzioni:
                        msg = '\U0001f4c5 *Ecco le disponibilita\' per quel giorno:*\n'
                        for i, opt in enumerate(opzioni, 1):
                            msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                        msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, opzioni)
                        return msg
                    return '\u26a0\ufe0f Nessuno slot disponibile per la data indicata. Scelga tra le opzioni proposte o indichi un\'altra data.'
                # Solo fascia oraria senza data: filtra le opzioni
                if preferred_period:
                    filtered = [opt for opt in pending_data if
                        (preferred_period == 'mattina' and opt.get('range_inizio', opt.get('time', '')) < '13:00') or
                        (preferred_period == 'pomeriggio' and opt.get('range_inizio', opt.get('time', '')) >= '13:00')]
                    if len(filtered) == 1:
                        opzione = filtered[0]
                        if 'range_inizio' in opzione:
                            date = datetime.strptime(opzione['date'], '%Y-%m-%d').replace(tzinfo=TZ_ROME)
                            tm = datetime(date.year, date.month, date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
                            tx = datetime(date.year, date.month, date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
                            bt = get_busy_times(tm, tx)
                            start_time = preferred_time if preferred_time else opzione['range_inizio']
                            tutti_slot = trova_tutti_slot_in_range(date, start_time, opzione['range_fine'], bt)
                            if not tutti_slot:
                                return '\u26a0\ufe0f Questa fascia non e\' piu\' disponibile. Scelga un\'altra opzione.'
                            if len(tutti_slot) == 1:
                                slot = tutti_slot[0]
                            else:
                                msg = '\U0001f552 *Orari disponibili:*\n'
                                for i, s in enumerate(tutti_slot, 1):
                                    msg += '  *' + str(i) + '.* ' + s['display'] + '\n'
                                msg += '\n\U0001f449 Risponda con il *numero* dell\'orario preferito.'
                                messages, _ = db.get_conversation(phone)
                                db.save_conversation(phone, messages, tutti_slot)
                                return msg
                        else:
                            slot = opzione
                        new_pending = {'state': 'waiting_name', 'slot': slot}
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, new_pending)
                        return '\U0001f4c5 Orario assegnato: *' + slot['display'] + '*\n\n\U0001f464 Per completare la prenotazione, mi dica *nome e cognome*.'
                    elif len(filtered) > 1:
                        msg = '\U0001f4c5 *Opzioni di ' + preferred_period + ':*\n'
                        for i, opt in enumerate(filtered, 1):
                            msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                        msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                        messages, _ = db.get_conversation(phone)
                        db.save_conversation(phone, messages, filtered)
                        return msg
            return None
    elif isinstance(pending_data, dict):
        state = pending_data.get('state', '')
        if state == 'waiting_name':
            # Fase 2: raccolta nome
            nome = scelta.strip()
            # Verifica se il cliente vuole cambiare orario invece di dare il nome
            if _rileva_intento_cambio(nome, 'mi dica nome e cognome'):
                opzioni = genera_opzioni_range()
                if not opzioni:
                    return '\u26a0\ufe0f Nessuno slot disponibile al momento.\nCi contatti al ' + TELEFONO + ' per fissare un appuntamento.'
                msg = '\U0001f4c5 *Quando puo\' portare l\'auto?*\n'
                for i, opt in enumerate(opzioni, 1):
                    msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, opzioni)
                return msg
            if len(nome) < 2:
                return '\u26a0\ufe0f Per favore, inserisca nome e cognome validi.'
            pending_data['nome_cliente'] = nome
            pending_data['state'] = 'waiting_car'
            messages, _ = db.get_conversation(phone)
            db.save_conversation(phone, messages, pending_data)
            return '\U0001f697 Quale veicolo porta in officina? (marca, modello e targa)\nEs: _Fiat Punto AB123CD_'
        elif state == 'waiting_car':
            # Fase 3: raccolta auto → riepilogo per conferma
            auto = scelta.strip()
            # Verifica se il cliente vuole cambiare orario invece di dare il veicolo
            if _rileva_intento_cambio(auto, 'quale veicolo porta in officina? (marca, modello e targa)'):
                opzioni = genera_opzioni_range()
                if not opzioni:
                    return '\u26a0\ufe0f Nessuno slot disponibile al momento.\nCi contatti al ' + TELEFONO + ' per fissare un appuntamento.'
                msg = '\U0001f4c5 *Quando puo\' portare l\'auto?*\n'
                for i, opt in enumerate(opzioni, 1):
                    msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, opzioni)
                return msg
            if len(auto) < 3:
                return '\u26a0\ufe0f Per favore, inserisca i dati del veicolo (marca, modello e targa).'
            pending_data['auto_cliente'] = auto
            pending_data['state'] = 'waiting_confirm'
            messages, _ = db.get_conversation(phone)
            db.save_conversation(phone, messages, pending_data)
            nome = pending_data.get('nome_cliente', '')
            slot_display = pending_data['slot'].get('display', '')
            msg = '\U0001f4cb *Riepilogo prenotazione:*\n\n'
            msg += '\U0001f464 *Nome:* ' + nome + '\n'
            msg += '\U0001f697 *Veicolo:* ' + auto + '\n'
            msg += '\U0001f4c5 *Appuntamento:* ' + slot_display + '\n'
            msg += '\U0001f4cd *Dove:* ' + INDIRIZZO + '\n\n'
            msg += 'Tutto corretto? Risponda:\n'
            msg += '  *1.* \u2705 Conferma\n'
            msg += '  *2.* \u270f\ufe0f Correggi nome\n'
            msg += '  *3.* \u270f\ufe0f Correggi veicolo\n'
            msg += '  *4.* \u270f\ufe0f Cambia orario'
            return msg
        elif state == 'waiting_confirm':
            # Fase 3b: conferma o correzione riepilogo
            risposta = scelta.strip().lower()
            if risposta in ['1', 'conferma', 'confermo', 'si', 'sì', 'ok', 'va bene', 'perfetto']:
                return conferma_prenotazione(phone, pending_data)
            elif risposta in ['2', 'correggi nome', 'nome']:
                pending_data['state'] = 'waiting_name'
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, pending_data)
                return '\U0001f464 Inserisca il *nome e cognome* corretto.'
            elif risposta in ['3', 'correggi veicolo', 'veicolo', 'auto']:
                pending_data['state'] = 'waiting_car'
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, pending_data)
                return '\U0001f697 Inserisca i dati corretti del veicolo (marca, modello e targa).'
            elif risposta in ['4', 'cambia orario', 'orario', 'data']:
                # Torna alla scelta slot
                opzioni = genera_opzioni_range()
                if not opzioni:
                    return '\u26a0\ufe0f Nessuno slot disponibile al momento. Risponda *1* per confermare l\'orario attuale.'
                msg = '\U0001f4c5 *Fasce orarie disponibili:*\n'
                for i, opt in enumerate(opzioni, 1):
                    msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                messages, _ = db.get_conversation(phone)
                db.save_conversation(phone, messages, opzioni)
                return msg
            else:
                return '\u26a0\ufe0f Risponda con *1* per confermare, *2* per correggere il nome, *3* per il veicolo, *4* per l\'orario.'
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
                # Mostra fasce orarie disponibili
                booking_id = pending_data['reschedule_booking_id']
                opzioni = genera_opzioni_range()
                if not opzioni:
                    db.clear_conversation(phone)
                    return '\u26a0\ufe0f Nessuno slot disponibile al momento. Riprovera\' piu\' tardi.'
                msg = '\U0001f4c5 *Fasce orarie disponibili:*\n'
                for i, opt in enumerate(opzioni, 1):
                    msg += '  *' + str(i) + '.* ' + opt['display'] + '\n'
                msg += '\n\U0001f449 Risponda con il *numero* della fascia preferita.'
                pending_data['state'] = 'reschedule_waiting_slot'
                pending_data['reschedule_slots'] = opzioni
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
                # Notifica officina della cancellazione
                if WHATSAPP_OFFICINA:
                    nome = pending_data.get('reschedule_nome', '')
                    auto = pending_data.get('reschedule_auto', '')
                    old_slot = pending_data.get('reschedule_slot_display', '')
                    notifica = '\u274c *Appuntamento Cancellato*\n\n'
                    notifica += '\U0001f464 *Cliente:* ' + nome + '\n'
                    notifica += '\U0001f697 *Veicolo:* ' + auto + '\n'
                    notifica += '\U0001f4de *Telefono:* ' + phone + '\n'
                    notifica += '\U0001f4c5 *Appuntamento cancellato:* ' + old_slot + '\n'
                    logger.info('Invio notifica cancellazione a ' + WHATSAPP_OFFICINA)
                    send_whatsapp_message(WHATSAPP_OFFICINA, notifica)
                return '\u2705 *Appuntamento cancellato.*\n\nSe avra\' bisogno in futuro, ci scriva pure qui in chat.\nA presto! \U0001f44b'
            else:
                return '\u26a0\ufe0f Risponda con *1* per spostare o *2* per cancellare.'
        elif state == 'reschedule_waiting_slot':
            # Spostamento: scelta fascia oraria
            try:
                idx = int(scelta.strip()) - 1
                opzioni = pending_data.get('reschedule_slots', [])
                if 0 <= idx < len(opzioni):
                    opzione = opzioni[idx]
                    if 'range_inizio' in opzione:
                        date = datetime.strptime(opzione['date'], '%Y-%m-%d').replace(tzinfo=TZ_ROME)
                        time_min = datetime(date.year, date.month, date.day, 0, 0, tzinfo=TZ_ROME).isoformat()
                        time_max = datetime(date.year, date.month, date.day, 23, 59, tzinfo=TZ_ROME).isoformat()
                        busy_times = get_busy_times(time_min, time_max)
                        tutti_slot = trova_tutti_slot_in_range(date, opzione['range_inizio'], opzione['range_fine'], busy_times)
                        if not tutti_slot:
                            return '\u26a0\ufe0f Questa fascia non e\' piu\' disponibile. Scelga un\'altra opzione.'
                        if len(tutti_slot) == 1:
                            new_slot = tutti_slot[0]
                        else:
                            msg = '\U0001f552 *Orari disponibili:*\n'
                            for i, s in enumerate(tutti_slot, 1):
                                msg += '  *' + str(i) + '.* ' + s['display'] + '\n'
                            msg += '\n\U0001f449 Risponda con il *numero* dell\'orario preferito.'
                            pending_data['reschedule_slots'] = tutti_slot
                            messages, _ = db.get_conversation(phone)
                            db.save_conversation(phone, messages, pending_data)
                            return msg
                    else:
                        new_slot = opzione
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
                    old_slot_display = pending_data.get('reschedule_slot_display', '')
                    db.update_booking(booking_id, new_slot, new_event_id)
                    db.clear_conversation(phone)
                    msg = '\u2705 *Appuntamento Spostato!*\n\n'
                    msg += '\U0001f464 *Cliente:* ' + nome + '\n'
                    msg += '\U0001f697 *Veicolo:* ' + auto + '\n'
                    msg += '\U0001f4c5 *Nuovo appuntamento:* ' + new_slot['display'] + '\n'
                    msg += '\U0001f4cd ' + INDIRIZZO + '\n\U0001f4de ' + TELEFONO
                    msg += '\n\nA presto! \U0001f44b'
                    # Notifica officina dello spostamento
                    if WHATSAPP_OFFICINA:
                        notifica = '\U0001f504 *Appuntamento Spostato*\n\n'
                        notifica += '\U0001f464 *Cliente:* ' + nome + '\n'
                        notifica += '\U0001f697 *Veicolo:* ' + auto + '\n'
                        notifica += '\U0001f4de *Telefono:* ' + phone + '\n'
                        notifica += '\U0001f4c5 *Vecchio appuntamento:* ' + old_slot_display + '\n'
                        notifica += '\U0001f4c5 *Nuovo appuntamento:* ' + new_slot['display'] + '\n'
                        logger.info('Invio notifica spostamento a ' + WHATSAPP_OFFICINA)
                        send_whatsapp_message(WHATSAPP_OFFICINA, notifica)
                    return msg
                else:
                    return '\u26a0\ufe0f Scelta non valida. Risponda con un numero da 1 a ' + str(len(opzioni)) + '.'
            except ValueError:
                return '\u26a0\ufe0f Risponda con il *numero* della fascia desiderata.'
    return None

RESCHEDULE_KEYWORDS = ['spostare', 'sposta', 'modificare', 'modifica', 'cambiare', 'cambia',
                        'riprogrammare', 'riprogramma', 'posticipare', 'posticipa', 'anticipare',
                        'anticipa', 'spostamento', 'cambio appuntamento', 'modifica appuntamento',
                        'cancellare', 'cancella', 'disdire', 'disdetta', 'annullare', 'annulla']

def is_reschedule_intent(msg):
    msg_lower = msg.lower()
    # Frasi esplicite con keyword + oggetto
    if any(kw in msg_lower for kw in RESCHEDULE_KEYWORDS) and any(
        w in msg_lower for w in ['appuntamento', 'prenotazione', 'data', 'orario', 'giorno', 'visita']):
        return True
    # Frasi compound che indicano chiaramente spostamento/cancellazione
    if any(phrase in msg_lower for phrase in [
        'spostare appuntamento', 'sposta appuntamento', 'cancella appuntamento',
        'disdire appuntamento', 'modifica appuntamento', 'cambio appuntamento',
        'spostare la prenotazione', 'cancellare la prenotazione',
        'vorrei spostare', 'vorrei cancellare', 'vorrei disdire', 'vorrei modificare',
        'devo spostare', 'devo cancellare', 'devo disdire',
        'posso spostare', 'posso cancellare', 'posso disdire',
        'si puo spostare', 'si puo cancellare', 'si può spostare', 'si può cancellare',
    ]):
        return True
    return False

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
    'Puo\' scrivermi liberamente o inviarmi un *messaggio vocale* spiegandomi in modo dettagliato il problema! \U0001f399\ufe0f')

INFO_MSG = ('\U0001f3e2 *' + NOME + '*\n\n'
    '\U0001f4cd *Indirizzo:* ' + INDIRIZZO + '\n'
    '\U0001f4de *Telefono:* ' + TELEFONO + '\n\n'
    '\U0001f552 *Orari di apertura:*\n'
    '  Lun - Mer: Mattina 9:00 - 12:00 | Pomeriggio 15:00 - 18:30\n\n'
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
    # Fallback AI: se le keyword non hanno catturato, chiedi a Haiku (solo se ha una prenotazione attiva)
    if not pending_slots:
        booking = db.find_active_booking_by_phone(from_number)
        if booking and _rileva_intento_modifica(incoming_msg):
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
        now_rome = datetime.now(TZ_ROME)
        oggi = now_rome.strftime('%d/%m/%Y')
        ora = now_rome.strftime('%H:%M')
        system_con_data = SYSTEM_PROMPT + '\n\nDATA E ORA ATTUALI: ' + oggi + ' ore ' + ora + '. Usa questa data per i calcoli interni sul periodo dalla consegna.'
        response = claude_client.messages.create(
            model='claude-sonnet-4-6', max_tokens=1000,
            system=system_con_data, messages=claude_msgs_for_api,
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
    # Se c'erano pending_slots attivi e non ne abbiamo creati di nuovi, preservali
    if pending_slots and new_pending is None:
        new_pending = pending_slots
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
        value = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {})
        # Ignora messaggi destinati ad altri numeri (es. numero di test usato da chatbot-leads)
        incoming_phone_id = value.get('metadata', {}).get('phone_number_id', '')
        if incoming_phone_id and incoming_phone_id != META_PHONE_NUMBER_ID:
            logger.info('Messaggio per altro numero (%s), ignorato', incoming_phone_id)
            return 'OK', 200
        msgs = value.get('messages', [])
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
                try:
                    lock = get_user_lock(phone)
                    with lock:
                        reply = process_message(phone, text, image=img)
                        send_whatsapp_message(phone, reply)
                except Exception as e:
                    logger.error('Thread errore per ' + phone + ': ' + str(e))
                    import traceback
                    logger.error(traceback.format_exc())
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
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Roboto', sans-serif; background: #111; display: flex;
               justify-content: center; align-items: center; min-height: 100vh; }
        .login-box { background: #1a1a1a; padding: 48px 40px; border-radius: 16px;
                     box-shadow: 0 8px 32px rgba(0,0,0,0.4); width: 90%%; max-width: 420px;
                     text-align: center; border: 1px solid #2a2a2a; }
        .logo { margin-bottom: 24px; }
        .logo img { height: 40px; }
        .login-box h2 { font-size: 18px; color: #999; font-weight: 400; margin-bottom: 32px; }
        input[type="password"] { width: 100%%; padding: 14px 16px; background: #222; border: 1px solid #333;
                                  border-radius: 8px; font-size: 16px; margin-bottom: 16px; color: white;
                                  font-family: 'Roboto', sans-serif; }
        input[type="password"]:focus { outline: none; border-color: #be1010; }
        input[type="password"]::placeholder { color: #666; }
        button { width: 100%%; padding: 14px; background: #be1010; color: white; border: none;
                 border-radius: 9999px; font-size: 16px; font-weight: 700; cursor: pointer;
                 font-family: 'Roboto', sans-serif; transition: background 0.2s; }
        button:hover { background: #a00d0d; }
        .error { color: #ff4444; margin-bottom: 16px; font-size: 14px; }
    </style>
</head>
<body>
    <div class="login-box">
        <div class="logo">
            <img src="https://cdnwp.dealerk.com/eed49ed7/uploads/sites/1480/2026/03/logo-4-0x50.png" alt="''' + NOME + '''">
            <span class="title">Dashboard Officina</span>
        </div>
        <h2>Area Officina</h2>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="POST">
            <input type="password" name="password" placeholder="Password" required autofocus>
            <button type="submit">Accedi</button>
        </form>
    </div>
</body>
</html>
'''

ADMIN_SIDEBAR_CSS = '''
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Roboto', sans-serif; background: #111; min-height: 100vh; display: flex; }
        .sidebar { width: 220px; background: #0a0a0a; border-right: 1px solid #222; min-height: 100vh;
                   padding: 24px 16px; position: fixed; top: 0; left: 0; display: flex; flex-direction: column; }
        .sidebar .logo { margin-bottom: 8px; }
        .sidebar .logo img { height: 32px; }
        .sidebar .subtitle { color: #555; font-size: 11px; font-weight: 500; letter-spacing: 0.5px;
                              text-transform: uppercase; margin-bottom: 32px; }
        .sidebar .nav { display: flex; flex-direction: column; gap: 4px; flex: 1; }
        .sidebar .nav a { color: #888; text-decoration: none; font-size: 14px; font-weight: 500;
                          padding: 10px 14px; border-radius: 8px; transition: all 0.2s; display: block; }
        .sidebar .nav a:hover { color: white; background: #1a1a1a; }
        .sidebar .nav a.active { color: white; background: #be1010; }
        .sidebar .nav a.logout { color: #555; font-size: 13px; margin-top: auto; }
        .sidebar .nav a.logout:hover { color: #ff4444; background: transparent; }
        .main { margin-left: 220px; flex: 1; padding: 24px; }
        .container { max-width: 800px; margin: 0 auto; }
        @media (max-width: 768px) {
            .sidebar { width: 100%%; min-height: auto; position: relative; flex-direction: row;
                       padding: 12px 16px; align-items: center; gap: 12px; }
            .sidebar .logo { margin-bottom: 0; }
            .sidebar .subtitle { display: none; }
            .sidebar .nav { flex-direction: row; gap: 4px; flex-wrap: wrap; }
            .sidebar .nav a { padding: 6px 12px; font-size: 13px; }
            .sidebar .nav a.logout { margin-top: 0; }
            .main { margin-left: 0; }
            body { flex-direction: column; }
        }
'''

ADMIN_SIDEBAR_HTML = '''
    <div class="sidebar">
        <div class="logo">
            <img src="https://cdnwp.dealerk.com/eed49ed7/uploads/sites/1480/2026/03/logo-4-0x50.png" alt="''' + NOME + '''">
        </div>
        <div class="subtitle">Dashboard Officina</div>
        <div class="nav">
            <a href="/admin/dashboard" class="{{ 'active' if page == 'prenotazioni' else '' }}">Prenotazioni</a>
            <a href="/admin/completate" class="{{ 'active' if page == 'completate' else '' }}">Completate</a>
            <a href="/admin/archivio" class="{{ 'active' if page == 'archivio' else '' }}">Archivio</a>
            <a href="/admin/logout" class="logout">Esci</a>
        </div>
    </div>
'''

ADMIN_PAGE_HTML = '''
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Officina - ''' + NOME + '''</title>
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
''' + ADMIN_SIDEBAR_CSS + '''
        .page-title { color: white; font-size: 22px; font-weight: 700; margin-bottom: 20px; }
        .card { background: #1a1a1a; border-radius: 12px; border: 1px solid #2a2a2a;
                margin-bottom: 12px; padding: 18px; display: flex; justify-content: space-between;
                align-items: center; flex-wrap: wrap; gap: 12px; transition: border-color 0.2s; }
        .card:hover { border-color: #be1010; }
        .card-info { flex: 1; min-width: 200px; }
        .card-info .name { font-weight: 700; font-size: 16px; color: white; }
        .card-info .details { font-size: 14px; color: #888; margin-top: 4px; }
        .card-info .slot { font-size: 14px; color: #be1010; font-weight: 500; margin-top: 4px; }
        .card-info .problem { font-size: 13px; color: #666; margin-top: 4px; font-style: italic; }
        .card-photos { display: flex; gap: 6px; margin-top: 8px; flex-wrap: wrap; }
        .card-photos img { width: 60px; height: 60px; object-fit: cover; border-radius: 6px;
                           border: 1px solid #333; cursor: pointer; transition: transform 0.2s; }
        .card-photos img:hover { transform: scale(1.1); }
        .photo-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
                       background: rgba(0,0,0,0.9); z-index: 1000; justify-content: center; align-items: center; }
        .photo-modal img { max-width: 90%; max-height: 90%; border-radius: 8px; }
        .photo-modal.active { display: flex; }
        .btn-group { display: flex; flex-direction: column; gap: 8px; }
        .btn-avvisa { padding: 10px 20px; background: #25D366; color: white; border: none;
                      border-radius: 9999px; font-size: 13px; font-weight: 700; cursor: pointer;
                      white-space: nowrap; font-family: 'Roboto', sans-serif; transition: background 0.2s; }
        .btn-avvisa:hover { background: #1da851; }
        .btn-avvisa:disabled { background: #333; color: #666; cursor: not-allowed; }
        .btn-non-pronta { padding: 10px 20px; background: #333; color: #ccc; border: 1px solid #444;
                          border-radius: 9999px; font-size: 13px; font-weight: 700; cursor: pointer;
                          white-space: nowrap; font-family: 'Roboto', sans-serif; transition: all 0.2s; }
        .btn-non-pronta:hover { background: #e67e22; color: white; border-color: #e67e22; }
        .btn-non-pronta:disabled { background: #222; color: #555; cursor: not-allowed; border-color: #333; }
        .empty { text-align: center; padding: 60px 20px; color: #555; font-size: 16px; }
        .flash { max-width: 800px; margin: 12px auto; padding: 12px 20px; border-radius: 8px;
                 font-size: 14px; }
        .flash.success { background: #0d2e1a; color: #4caf50; border: 1px solid #1b5e20; }
        .flash.error { background: #2e0d0d; color: #ff4444; border: 1px solid #5e1b1b; }
        .coverage { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 11px;
                    font-weight: 700; margin-left: 8px; text-transform: uppercase; letter-spacing: 0.5px; }
        .coverage.check_qualita { background: #1a2a3a; color: #4da6ff; border: 1px solid #2a4a6a; }
        .coverage.garanzia_ordinaria { background: #2a2a1a; color: #e6a817; border: 1px solid #4a4a2a; }
    </style>
</head>
<body>
''' + ADMIN_SIDEBAR_HTML + '''
    <div class="main">
    {% if flash_msg %}
    <div class="flash">{{ flash_msg }}</div>
    {% endif %}
    <div class="container">
        {% if bookings %}
            {% for b in bookings %}
            <div class="card">
                <div class="card-info">
                    <div class="name">
                        {{ b.nome_cliente or b.customer_name or 'Cliente' }}
                        {% if b.coverage_type %}
                        <span class="coverage {{ b.coverage_type }}">{{ "Check Qualita" if b.coverage_type == "check_qualita" else "Garanzia" }}</span>
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
                                  flash_msg=flash_msg, flash_type=flash_type, page='prenotazioni')


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
    booking_id = request.form.get('booking_id', '')
    ok, err = send_template_message(phone, TEMPLATE_NAME, [nome, auto])
    if ok:
        if booking_id:
            db.complete_booking(int(booking_id))
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


ADMIN_COMPLETATE_HTML = '''
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Completate - ''' + NOME + '''</title>
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
''' + ADMIN_SIDEBAR_CSS + '''
        .page-title { color: white; font-size: 22px; font-weight: 700; margin-bottom: 20px; }
        .card { background: #1a1a1a; border-radius: 12px; border: 1px solid #2a2a2a; padding: 18px;
                margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        .card-info { flex: 1; }
        .card-info .name { font-weight: 700; font-size: 16px; color: white; }
        .card-info .details { font-size: 14px; color: #888; margin-top: 4px; }
        .card-info .slot { font-size: 14px; color: #be1010; font-weight: 500; margin-top: 4px; }
        .card-info .problem { font-size: 13px; color: #666; margin-top: 4px; }
        .coverage { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 11px;
                    font-weight: 700; margin-left: 8px; text-transform: uppercase; letter-spacing: 0.5px; }
        .coverage.check_qualita { background: #1a2a3a; color: #4da6ff; border: 1px solid #2a4a6a; }
        .coverage.garanzia_ordinaria { background: #2a2a1a; color: #e6a817; border: 1px solid #4a4a2a; }
        .badge-done { background: #0d2e1a; color: #4caf50; padding: 6px 14px; border-radius: 9999px;
                      font-size: 12px; font-weight: 700; white-space: nowrap; border: 1px solid #1b5e20;
                      text-transform: uppercase; letter-spacing: 0.5px; }
        .empty { text-align: center; padding: 60px 20px; color: #555; font-size: 16px; }
        .btn-archivia { padding: 6px 14px; background: transparent; color: #666; border: 1px solid #333;
                        border-radius: 9999px; font-size: 12px; font-weight: 500; cursor: pointer;
                        font-family: 'Roboto', sans-serif; transition: all 0.2s; }
        .btn-archivia:hover { color: white; border-color: #be1010; background: #be1010; }
        .btn-archivia:disabled { color: #444; border-color: #222; cursor: not-allowed; }
    </style>
</head>
<body>
''' + ADMIN_SIDEBAR_HTML + '''
    <div class="main">
    <div class="container">
        <div class="page-title">Prenotazioni Completate</div>
        {% if bookings %}
            {% for b in bookings %}
            <div class="card">
                <div class="card-info">
                    <div class="name">
                        {{ b.nome_cliente or b.customer_name or 'Cliente' }}
                        {% if b.coverage_type %}
                        <span class="coverage {{ b.coverage_type }}">{{ "Check Qualita" if b.coverage_type == "check_qualita" else "Garanzia" }}</span>
                        {% endif %}
                    </div>
                    <div class="details">&#x1f697; {{ b.auto_cliente or 'N/D' }}</div>
                    <div class="slot">&#x1f4c5; {{ b.slot_display or 'N/D' }}</div>
                    {% if b.summary %}
                    <div class="problem">&#x1f4dd; {{ b.summary }}</div>
                    {% endif %}
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <div class="badge-done">Completata</div>
                    <form method="POST" action="/admin/archivia">
                        <input type="hidden" name="booking_id" value="{{ b.id }}">
                        <button type="submit" class="btn-archivia"
                                onclick="this.disabled=true; this.innerText='...'; this.form.submit();">
                            Archivia
                        </button>
                    </form>
                </div>
            </div>
            {% endfor %}
        {% else %}
            <div class="empty">Nessuna prenotazione completata.</div>
        {% endif %}
    </div>
    </div>
</body>
</html>
'''


@app.route('/admin/completate')
def admin_completate():
    if not session.get('admin'):
        return redirect('/admin')
    bookings = db.get_completed_bookings()
    return render_template_string(ADMIN_COMPLETATE_HTML, bookings=bookings, page='completate')


@app.route('/admin/archivia', methods=['POST'])
def admin_archivia():
    if not session.get('admin'):
        return redirect('/admin')
    booking_id = request.form.get('booking_id', '')
    if booking_id:
        db.archive_booking(int(booking_id))
    return redirect('/admin/completate?msg=Prenotazione+archiviata&type=success')


ADMIN_ARCHIVIO_HTML = '''
<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Archivio - ''' + NOME + '''</title>
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
''' + ADMIN_SIDEBAR_CSS + '''
        .page-title { color: white; font-size: 22px; font-weight: 700; margin-bottom: 20px; }
        .filters { display: flex; gap: 8px; margin-bottom: 20px; flex-wrap: wrap; }
        .filters a { color: #999; text-decoration: none; font-size: 13px; font-weight: 500;
                     padding: 6px 14px; border-radius: 9999px; border: 1px solid #333;
                     transition: all 0.2s; }
        .filters a:hover { color: white; border-color: #be1010; }
        .filters a.active { color: white; background: #be1010; border-color: #be1010; }
        .card { background: #1a1a1a; border-radius: 12px; border: 1px solid #2a2a2a; padding: 18px;
                margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        .card-info { flex: 1; }
        .card-info .name { font-weight: 700; font-size: 16px; color: white; }
        .card-info .details { font-size: 14px; color: #888; margin-top: 4px; }
        .card-info .slot { font-size: 14px; color: #be1010; font-weight: 500; margin-top: 4px; }
        .card-info .problem { font-size: 13px; color: #666; margin-top: 4px; }
        .coverage { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 11px;
                    font-weight: 700; margin-left: 8px; text-transform: uppercase; letter-spacing: 0.5px; }
        .coverage.check_qualita { background: #1a2a3a; color: #4da6ff; border: 1px solid #2a4a6a; }
        .coverage.garanzia_ordinaria { background: #2a2a1a; color: #e6a817; border: 1px solid #4a4a2a; }
        .badge-done { background: #0d2e1a; color: #4caf50; padding: 6px 14px; border-radius: 9999px;
                      font-size: 12px; font-weight: 700; white-space: nowrap; border: 1px solid #1b5e20;
                      text-transform: uppercase; letter-spacing: 0.5px; }
        .empty { text-align: center; padding: 60px 20px; color: #555; font-size: 16px; }
        .count { color: #666; font-size: 14px; margin-bottom: 16px; }
    </style>
</head>
<body>
''' + ADMIN_SIDEBAR_HTML + '''
    <div class="main">
    <div class="container">
        <div class="page-title">Archivio Prenotazioni</div>
        {% if mesi %}
        <div class="filters">
            <a href="/admin/archivio" class="{{ 'active' if not mese_sel else '' }}">Tutti</a>
            {% for m in mesi %}
            <a href="/admin/archivio?mese={{ m }}" class="{{ 'active' if mese_sel == m else '' }}">{{ m }}</a>
            {% endfor %}
        </div>
        {% endif %}
        <div class="count">{{ bookings|length }} prenotazioni</div>
        {% if bookings %}
            {% for b in bookings %}
            <div class="card">
                <div class="card-info">
                    <div class="name">
                        {{ b.nome_cliente or b.customer_name or 'Cliente' }}
                        {% if b.coverage_type %}
                        <span class="coverage {{ b.coverage_type }}">{{ "Check Qualita" if b.coverage_type == "check_qualita" else "Garanzia" }}</span>
                        {% endif %}
                    </div>
                    <div class="details">&#x1f697; {{ b.auto_cliente or 'N/D' }}</div>
                    <div class="slot">&#x1f4c5; {{ b.slot_display or 'N/D' }}</div>
                    {% if b.summary %}
                    <div class="problem">&#x1f4dd; {{ b.summary }}</div>
                    {% endif %}
                </div>
                <div class="badge-done">Completata</div>
            </div>
            {% endfor %}
        {% else %}
            <div class="empty">Nessuna prenotazione in archivio.</div>
        {% endif %}
    </div>
    </div>
</body>
</html>
'''


@app.route('/admin/archivio')
def admin_archivio():
    if not session.get('admin'):
        return redirect('/admin')
    mese = request.args.get('mese')
    mesi = db.get_completed_months()
    bookings = db.get_all_completed_bookings(mese)
    return render_template_string(ADMIN_ARCHIVIO_HTML, bookings=bookings, mesi=mesi, mese_sel=mese, page='archivio')


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin', None)
    return redirect('/admin')



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
                       t.category, t.coverage_type, t.summary, t.recommendation
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
                       t.category, t.coverage_type, t.summary
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
                SELECT id, phone, category, coverage_type, summary, recommendation,
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
    """Distribuzione per tipo copertura."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.coverage_type, COUNT(*) as totale
                FROM bookings b
                JOIN triage_results t ON b.triage_id = t.id
                GROUP BY t.coverage_type ORDER BY totale DESC
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


# --- PROMEMORIA AUTOMATICI ---

def invia_promemoria_domani():
    """Invia promemoria per gli appuntamenti di domani (eseguito alle 18:00)."""
    logger.info('Avvio invio promemoria giorno prima...')
    bookings = db.get_tomorrow_bookings()
    for b in bookings:
        nome = b.get('nome_cliente', 'Cliente')
        auto = b.get('auto_cliente', '')
        slot = b.get('slot_display', '')
        phone = b['phone']
        try:
            ok, err = send_template_message(phone, TEMPLATE_PROMEMORIA, [nome, slot])
            if ok:
                db.mark_promemoria_sent(b['id'])
                logger.info('Promemoria domani inviato a ' + phone)
            else:
                logger.error('Errore promemoria domani per ' + phone + ': ' + err)
        except Exception as e:
            logger.error('Eccezione promemoria domani per ' + phone + ': ' + str(e))
    logger.info('Promemoria domani completati: ' + str(len(bookings)) + ' prenotazioni')


def invia_promemoria_oggi():
    """Invia promemoria per gli appuntamenti di oggi (eseguito alle 8:00)."""
    logger.info('Avvio invio promemoria giorno stesso...')
    bookings = db.get_today_bookings()
    for b in bookings:
        nome = b.get('nome_cliente', 'Cliente')
        auto = b.get('auto_cliente', '')
        slot = b.get('slot_display', '')
        phone = b['phone']
        try:
            ok, err = send_template_message(phone, TEMPLATE_PROMEMORIA_GIORNO, [nome, slot])
            if ok:
                db.mark_promemoria_giorno_sent(b['id'])
                logger.info('Promemoria oggi inviato a ' + phone)
            else:
                logger.error('Errore promemoria oggi per ' + phone + ': ' + err)
        except Exception as e:
            logger.error('Eccezione promemoria oggi per ' + phone + ': ' + str(e))
    logger.info('Promemoria oggi completati: ' + str(len(bookings)) + ' prenotazioni')


# Inizializza il database all'avvio
db.init_db()

# Avvia scheduler per promemoria automatici
scheduler = BackgroundScheduler(timezone='Europe/Rome')
scheduler.add_job(invia_promemoria_domani, 'cron', hour=18, minute=0, id='promemoria_domani')
scheduler.add_job(invia_promemoria_oggi, 'cron', hour=8, minute=0, id='promemoria_oggi')
scheduler.start()
logger.info('Scheduler promemoria avviato (domani h18:00, oggi h08:00)')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info('\U0001f680 ' + NOME + ' avviato sulla porta ' + str(port))
    app.run(host='0.0.0.0', port=port)
