import os
import json
import re
import logging
import threading
from datetime import datetime, timedelta
from flask import Flask, request
import anthropic
import requests as req

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CREDENZIALI (da variabili d'ambiente su Render) ---
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
META_ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', '')
META_PHONE_NUMBER_ID = os.environ.get('META_PHONE_NUMBER_ID', '')
META_VERIFY_TOKEN = os.environ.get('META_VERIFY_TOKEN', 'chatbot_officina_2024')

# --- CONFIG CONCESSIONARIO ---
NOME = os.environ.get('NOME_CONCESSIONARIO', 'AutoPlus')
INDIRIZZO = os.environ.get('INDIRIZZO', 'Via Roma 123, 80100 Napoli')
TELEFONO = os.environ.get('TELEFONO_OFFICINA', '+39 081 123 4567')

ORARIO_APERTURA = os.environ.get('ORARIO_APERTURA', '08:30')
ORARIO_CHIUSURA = os.environ.get('ORARIO_CHIUSURA', '17:00')
PAUSA_INIZIO = os.environ.get('PAUSA_PRANZO_INIZIO', '13:00')
PAUSA_FINE = os.environ.get('PAUSA_PRANZO_FINE', '14:00')
SABATO_CHIUSURA = os.environ.get('SABATO_CHIUSURA', '13:00')
DURATA_SLOT = int(os.environ.get('DURATA_SLOT_MINUTI', '60'))

# --- CLIENTS ---
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

META_API_URL = 'https://graph.facebook.com/v21.0/' + META_PHONE_NUMBER_ID + '/messages'
META_HEADERS = {
    'Authorization': 'Bearer ' + META_ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

# --- GOOGLE CALENDAR (opzionale) ---
calendar_service = None
GOOGLE_CALENDAR_ID = os.environ.get('GOOGLE_CALENDAR_ID', 'primary')
GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
if GOOGLE_CREDS:
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(GOOGLE_CREDS)
            creds_file = f.name
        credentials = Credentials.from_service_account_file(
            creds_file, scopes=['https://www.googleapis.com/auth/calendar']
        )
        calendar_service = build('calendar', 'v3', credentials=credentials)
        logger.info('Google Calendar connesso!')
    except Exception as e:
        logger.warning('Google Calendar non disponibile: ' + str(e))

# --- STORAGE ---
conversations = {}
triage_data_store = {}

def get_conversation(phone):
    return conversations.get(phone, [])

def save_conversation(phone, history):
    conversations[phone] = history

def clear_conversation(phone):
    conversations.pop(phone, None)

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
SYSTEM_PROMPT = '''Sei l'assistente virtuale dell'officina del concessionario ''' + NOME + '''.
Indirizzo officina: ''' + INDIRIZZO + '''
Telefono officina: ''' + TELEFONO + '''

Il tuo compito e':
1. ACCOGLIERE il cliente in modo cordiale e professionale
2. CAPIRE il problema con domande mirate (max 2-3 domande)
3. CLASSIFICARE la gravita' e proporre l'appuntamento

REGOLE:
- Sii cordiale ma conciso, max 2-3 frasi per messaggio
- Fai UNA domanda alla volta
- Linguaggio semplice, dai del "Lei"

QUANDO HAI ABBASTANZA INFO (dopo 2-3 scambi), rispondi SOLO con JSON:
{"triage_complete":true,"priority":"CRITICA|ALTA|MEDIA|BASSA","category":"motore|trasmissione|freni|sterzo|sospensioni|impianto_elettrico|climatizzazione|carrozzeria|pneumatici|luci|tergicristalli|batteria|scarico|tagliando|altro","summary":"Breve descrizione","recommendation":"Cosa consigliamo"}

PRIORITA':
- CRITICA: Sicurezza compromessa, veicolo non guidabile
- ALTA: Problema serio ma utilizzabile con cautela
- MEDIA: Da risolvere ma non urgente
- BASSA: Manutenzione ordinaria o estetica

NON classificare al primo messaggio. Fai ALMENO 1-2 domande prima.'''

# --- SLOT E PRIORITA' ---
PRIORITY_CONFIG = {
    'CRITICA': {'emoji': '🔴', 'label': 'URGENTE', 'days_range': (1, 2), 'slots': 3},
    'ALTA':    {'emoji': '🟠', 'label': 'PRIORITARIO', 'days_range': (2, 4), 'slots': 3},
    'MEDIA':   {'emoji': '🟡', 'label': 'NORMALE', 'days_range': (3, 7), 'slots': 3},
    'BASSA':   {'emoji': '🟢', 'label': 'PROGRAMMABILE', 'days_range': (5, 14), 'slots': 3},
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

def get_slot_da_calendario(priority):
    if calendar_service is None:
        return None
    try:
        config = PRIORITY_CONFIG[priority]
        start_day, end_day = config['days_range']
        now = datetime.now()
        time_min = (now + timedelta(days=start_day)).replace(hour=0, minute=0, second=0).isoformat() + 'Z'
        time_max = (now + timedelta(days=end_day + 1)).replace(hour=0, minute=0, second=0).isoformat() + 'Z'
        events_result = calendar_service.events().list(
            calendarId=GOOGLE_CALENDAR_ID, timeMin=time_min, timeMax=time_max,
            singleEvents=True, orderBy='startTime'
        ).execute()
        busy = []
        for ev in events_result.get('items', []):
            s = ev['start'].get('dateTime', '')
            e = ev['end'].get('dateTime', '')
            if s and e:
                busy.append((s, e))
        available = []
        for delta in range(start_day, end_day + 1):
            date = now + timedelta(days=delta)
            if date.weekday() not in GIORNI_LAVORATIVI:
                continue
            for orario in genera_orari_giornata(date.weekday()):
                h, m = map(int, orario.split(':'))
                ss = datetime(date.year, date.month, date.day, h, m)
                se = ss + timedelta(minutes=DURATA_SLOT)
                si, ei = ss.isoformat(), se.isoformat()
                if not any(si < be and ei > bs for bs, be in busy):
                    g = GIORNI[date.weekday()]
                    me = MESI[date.month - 1]
                    available.append({
                        'display': g + ' ' + str(date.day) + ' ' + me + ' ore ' + orario,
                        'date': date.strftime('%Y-%m-%d'), 'time': orario,
                        'datetime_start': si, 'datetime_end': ei,
                    })
        n = config['slots']
        if len(available) <= n:
            return available
        step = len(available) // n
        return [available[i * step] for i in range(n)]
    except Exception as e:
        logger.error('Calendar errore: ' + str(e))
        return None

def genera_slot(priority):
    cal = get_slot_da_calendario(priority)
    if cal is not None:
        return cal
    config = PRIORITY_CONFIG[priority]
    start, end = config['days_range']
    slots = []
    now = datetime.now()
    for delta in range(start, end + 1):
        date = now + timedelta(days=delta)
        if date.weekday() not in GIORNI_LAVORATIVI:
            continue
        for orario in genera_orari_giornata(date.weekday()):
            g = GIORNI[date.weekday()]
            m = MESI[date.month - 1]
            slots.append({
                'display': g + ' ' + str(date.day) + ' ' + m + ' ore ' + orario,
                'date': date.strftime('%Y-%m-%d'), 'time': orario,
            })
    n = config['slots']
    if len(slots) <= n:
        return slots
    step = len(slots) // n
    return [slots[i * step] for i in range(n)]

def crea_evento_calendario(slot, triage, phone):
    if calendar_service is None:
        return
    try:
        if 'datetime_start' in slot:
            start, end = slot['datetime_start'], slot['datetime_end']
        else:
            start = slot['date'] + 'T' + slot['time'] + ':00'
            h, m = map(int, slot['time'].split(':'))
            end = slot['date'] + 'T' + str(h+1).zfill(2) + ':' + str(m).zfill(2) + ':00'
        color_map = {'CRITICA': '11', 'ALTA': '6', 'MEDIA': '5', 'BASSA': '10'}
        event = {
            'summary': triage['priority'] + ' - ' + triage['category'] + ' - ' + phone[-10:],
            'description': 'Problema: ' + triage['summary'] + '\nConsiglio: ' + triage['recommendation'] + '\nTel: ' + phone,
            'start': {'dateTime': start, 'timeZone': 'Europe/Rome'},
            'end': {'dateTime': end, 'timeZone': 'Europe/Rome'},
            'colorId': color_map.get(triage['priority'], '1'),
        }
        calendar_service.events().insert(calendarId=GOOGLE_CALENDAR_ID, body=event).execute()
    except Exception as e:
        logger.error('Evento errore: ' + str(e))

def formatta_triage(triage):
    pri = triage['priority']
    config = PRIORITY_CONFIG[pri]
    slots = genera_slot(pri)
    msg = config['emoji'] + ' *VALUTAZIONE: Priorita\' ' + config['label'] + '*\n\n'
    msg += '📋 *Problema:* ' + triage['summary'] + '\n'
    msg += '💡 *Consiglio:* ' + triage['recommendation'] + '\n\n'
    msg += '📅 *Slot disponibili:*\n'
    for i, slot in enumerate(slots, 1):
        msg += '  *' + str(i) + '.* ' + slot['display'] + '\n'
    msg += '\n👉 Risponda con il *numero* dello slot (es. "1")\n'
    msg += '\n_Oppure "operatore" per parlare con un addetto._'
    return msg, slots

def gestisci_prenotazione(phone, scelta, pending_slots):
    try:
        idx = int(scelta.strip()) - 1
        if 0 <= idx < len(pending_slots):
            slot = pending_slots[idx]
            triage = triage_data_store.get(phone, {})
            crea_evento_calendario(slot, triage, phone)
            clear_conversation(phone)
            triage_data_store.pop(phone, None)
            return '✅ *Appuntamento Confermato!*\n\n📅 ' + slot['display'] + '\n📍 ' + INDIRIZZO + '\n📞 ' + TELEFONO + '\n\nRicevera\' un promemoria il giorno prima.\nGrazie e a presto! 👋'
        else:
            return '⚠️ Scelta non valida. Risponda con 1, 2 o 3.'
    except ValueError:
        return None

def process_message(from_number, incoming_msg):
    logger.info('📩 ' + from_number + ': ' + incoming_msg)
    if incoming_msg.lower() in ['reset', 'ricomincia', 'riparti']:
        clear_conversation(from_number)
        return '👋 Conversazione resettata! Mi dica pure, come posso aiutarla?'
    history = get_conversation(from_number)
    if history and isinstance(history[-1], dict) and history[-1].get('_pending_slots'):
        result = gestisci_prenotazione(from_number, incoming_msg, history[-1]['_pending_slots'])
        if result:
            return result
    claude_msgs = [m for m in history if isinstance(m, dict) and 'role' in m and not m.get('_pending_slots')]
    claude_msgs.append({'role': 'user', 'content': incoming_msg})
    try:
        response = claude_client.messages.create(
            model='claude-sonnet-4-20250514', max_tokens=1000,
            system=SYSTEM_PROMPT, messages=claude_msgs,
        )
        reply = response.content[0].text
    except Exception as e:
        logger.error('Claude errore: ' + str(e))
        return 'Mi scusi, problema tecnico. Riprovi tra qualche secondo.'
    reply_text = reply
    new_entry = {'role': 'assistant', 'content': reply}
    try:
        match = re.search(r'\{[\s\S]*"triage_complete"\s*:\s*true[\s\S]*\}', reply)
        if match:
            triage = json.loads(match.group(0))
            triage_data_store[from_number] = triage
            reply_text, slots = formatta_triage(triage)
            new_entry = {'_pending_slots': slots}
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning('Parsing: ' + str(e))
    save_conversation(from_number, claude_msgs + [new_entry])
    return reply_text

# --- FLASK ---
app = Flask(__name__)

@app.route('/', methods=['GET'])
def health():
    return {'status': 'ok', 'service': NOME, 'calendar': 'on' if calendar_service else 'off', 'chats': len(conversations)}, 200

@app.route('/webhook', methods=['GET'])
def verify():
    if request.args.get('hub.mode') == 'subscribe' and request.args.get('hub.verify_token') == META_VERIFY_TOKEN:
        return request.args.get('hub.challenge', ''), 200
    return 'Forbidden', 403

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    try:
        msgs = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('messages', [])
        if not msgs:
            return 'OK', 200
        msg = msgs[0]
        phone = msg.get('from', '')
        if msg.get('type') == 'text':
            text = msg.get('text', {}).get('body', '').strip()
            threading.Thread(target=lambda: send_whatsapp_message(phone, process_message(phone, text)), daemon=True).start()
        else:
            send_whatsapp_message(phone, 'Posso elaborare solo messaggi di testo.')
    except Exception as e:
        logger.error('Webhook errore: ' + str(e))
    return 'OK', 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info('🚀 ' + NOME + ' avviato sulla porta ' + str(port))
    app.run(host='0.0.0.0', port=port)
