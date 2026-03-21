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
import requests as req

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CREDENZIALI (da variabili d'ambiente su Render) ---
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
META_ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', '')
META_PHONE_NUMBER_ID = os.environ.get('META_PHONE_NUMBER_ID', '')
META_VERIFY_TOKEN = os.environ.get('META_VERIFY_TOKEN', 'chatbot_officina_2024')
META_APP_SECRET = os.environ.get('META_APP_SECRET', '')

# --- CONFIG CONCESSIONARIO ---
NOME = os.environ.get('NOME_CONCESSIONARIO', 'AutoPlus')
INDIRIZZO = os.environ.get('INDIRIZZO', 'Via Roma 123, 80100 Napoli')
TELEFONO = os.environ.get('TELEFONO_OFFICINA', '+39 081 123 4567')
WHATSAPP_OFFICINA = os.environ.get('WHATSAPP_OFFICINA', '393312782211')

ORARIO_APERTURA = os.environ.get('ORARIO_APERTURA', '08:30')
ORARIO_CHIUSURA = os.environ.get('ORARIO_CHIUSURA', '17:00')
PAUSA_INIZIO = os.environ.get('PAUSA_PRANZO_INIZIO', '13:00')
PAUSA_FINE = os.environ.get('PAUSA_PRANZO_FINE', '14:00')
SABATO_CHIUSURA = os.environ.get('SABATO_CHIUSURA', '13:00')
DURATA_SLOT = int(os.environ.get('DURATA_SLOT_MINUTI', '60'))
TZ_ROME = ZoneInfo('Europe/Rome')

# --- CLIENTS ---
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

META_API_URL = 'https://graph.facebook.com/v21.0/' + META_PHONE_NUMBER_ID + '/messages'
META_HEADERS = {
    'Authorization': 'Bearer ' + META_ACCESS_TOKEN,
    'Content-Type': 'application/json'
}


# --- DEDUPLICAZIONE MESSAGGI ---
processed_messages = {}

def is_duplicate(msg_id):
    """Evita di processare lo stesso messaggio due volte."""
    if not msg_id:
        return False
    if msg_id in processed_messages:
        return True
    now = datetime.now()
    to_remove = [k for k, v in processed_messages.items() if (now - v).total_seconds() > 300]
    for k in to_remove:
        processed_messages.pop(k, None)
    processed_messages[msg_id] = now
    return False

# --- LOCK PER UTENTE (evita risposte sovrapposte) ---
user_locks = {}
_locks_lock = threading.Lock()

def get_user_lock(phone):
    with _locks_lock:
        if phone not in user_locks:
            user_locks[phone] = threading.Lock()
        return user_locks[phone]

# --- STORAGE ---
conversations = {}
conversation_ts = {}
triage_data_store = {}
customer_names = {}
CONVERSATION_TTL = 3600

def cleanup_expired():
    now = datetime.now()
    expired = [k for k, v in conversation_ts.items() if (now - v).total_seconds() > CONVERSATION_TTL]
    for k in expired:
        conversations.pop(k, None)
        conversation_ts.pop(k, None)
        triage_data_store.pop(k, None)
        customer_names.pop(k, None)

def get_conversation(phone):
    return conversations.get(phone, [])

def save_conversation(phone, history):
    conversations[phone] = history
    conversation_ts[phone] = datetime.now()

def clear_conversation(phone):
    conversations.pop(phone, None)
    conversation_ts.pop(phone, None)

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

COMPITO: capire il problema auto del cliente e classificarlo per fissare un appuntamento.

REGOLE IMPORTANTI:
- Rispondi SEMPRE in 1-2 frasi brevi. MAI piu' di 3 frasi.
- NON salutare se il cliente ha gia' descritto un problema. Vai dritto alla domanda.
- Saluta SOLO se il cliente scrive "ciao" o un saluto generico senza descrivere problemi.
- Fai UNA domanda alla volta.
- Dai del "Lei".
- NON ripetere quello che il cliente ha detto.

DOPO 2-3 SCAMBI, rispondi SOLO con questo JSON (niente altro testo):
{"triage_complete":true,"priority":"CRITICA|ALTA|MEDIA|BASSA","category":"motore|trasmissione|freni|sterzo|sospensioni|impianto_elettrico|climatizzazione|carrozzeria|pneumatici|luci|tergicristalli|batteria|scarico|tagliando|altro","summary":"Breve descrizione","recommendation":"Cosa consigliamo"}

PRIORITA':
- CRITICA: veicolo non guidabile, sicurezza compromessa
- ALTA: problema serio ma utilizzabile con cautela
- MEDIA: da risolvere ma non urgente
- BASSA: manutenzione ordinaria o estetica

NON classificare al primo messaggio. Fai ALMENO 1 domanda prima.'''

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

SLOT_DAYS_AHEAD = 7

def genera_slot(priority):
    config = PRIORITY_CONFIG[priority]
    now = datetime.now(TZ_ROME)
    slots = []
    for delta in range(0, SLOT_DAYS_AHEAD + 1):
        date = now + timedelta(days=delta)
        if date.weekday() not in GIORNI_LAVORATIVI:
            continue
        for orario in genera_orari_giornata(date.weekday()):
            h, m = map(int, orario.split(':'))
            slot_start = datetime(date.year, date.month, date.day, h, m, tzinfo=TZ_ROME)
            if slot_start <= now:
                continue
            slot_end = slot_start + timedelta(minutes=DURATA_SLOT)
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
    msg += '📋 *Problema:* ' + triage['summary'] + '\n'
    msg += '💡 *Consiglio:* ' + triage['recommendation'] + '\n\n'
    if not slots:
        msg += '⚠️ Nessuno slot disponibile nei prossimi ' + str(SLOT_DAYS_AHEAD) + ' giorni.\n'
        msg += 'Ci contatti al ' + TELEFONO + ' per fissare un appuntamento.'
        return msg, []
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
            nome = customer_names.get(phone, phone)
            clear_conversation(phone)
            triage_data_store.pop(phone, None)
            customer_names.pop(phone, None)
            msg = '✅ *Appuntamento Confermato!*\n\n📅 ' + slot['display'] + '\n📍 ' + INDIRIZZO + '\n📞 ' + TELEFONO
            msg += '\n\nRicevera\' un promemoria il giorno prima.\nGrazie e a presto! 👋'
            if WHATSAPP_OFFICINA:
                notifica = '📋 *Nuova Prenotazione*\n\n'
                notifica += '👤 *Cliente:* ' + nome + '\n'
                notifica += '📞 *Telefono:* ' + phone + '\n'
                notifica += '📅 *Appuntamento:* ' + slot['display'] + '\n'
                if triage.get('priority'):
                    notifica += '🔧 *Priorita\':* ' + triage['priority'] + '\n'
                if triage.get('category'):
                    label = CATEGORY_LABELS.get(triage['category'], triage['category'])
                    notifica += '🏷️ *Categoria:* ' + label + '\n'
                if triage.get('summary'):
                    notifica += '📝 *Problema:* ' + triage['summary'] + '\n'
                if triage.get('recommendation'):
                    notifica += '💡 *Consiglio:* ' + triage['recommendation']
                send_whatsapp_message(WHATSAPP_OFFICINA, notifica)
            return msg
        else:
            return '⚠️ Scelta non valida. Risponda con un numero da 1 a ' + str(len(pending_slots)) + '.'
    except ValueError:
        return None

def process_message(from_number, incoming_msg):
    logger.info('📩 ' + from_number + ': ' + incoming_msg)
    cleanup_expired()
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
    return {'status': 'ok', 'service': NOME}, 200

@app.route('/webhook', methods=['GET'])
def verify():
    if request.args.get('hub.mode') == 'subscribe' and request.args.get('hub.verify_token') == META_VERIFY_TOKEN:
        return request.args.get('hub.challenge', ''), 200
    return 'Forbidden', 403

def verify_signature(req):
    if not META_APP_SECRET:
        return True
    signature = req.headers.get('X-Hub-Signature-256', '')
    if not signature.startswith('sha256='):
        return False
    expected = hmac.new(META_APP_SECRET.encode(), req.get_data(), hashlib.sha256).hexdigest()
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

        # Evita messaggi duplicati
        if is_duplicate(msg_id):
            logger.info('Duplicato ignorato: ' + msg_id)
            return 'OK', 200

        contacts = data.get('entry', [{}])[0].get('changes', [{}])[0].get('value', {}).get('contacts', [])
        if contacts:
            profile_name = contacts[0].get('profile', {}).get('name', '')
            if profile_name and phone:
                customer_names[phone] = profile_name

        if msg.get('type') == 'text':
            text = msg.get('text', {}).get('body', '').strip()
            def process_and_send():
                lock = get_user_lock(phone)
                with lock:
                    reply = process_message(phone, text)
                    send_whatsapp_message(phone, reply)
            threading.Thread(target=process_and_send, daemon=True).start()
        else:
            send_whatsapp_message(phone, 'Posso elaborare solo messaggi di testo.')
    except Exception as e:
        logger.error('Webhook errore: ' + str(e))
    return 'OK', 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info('🚀 ' + NOME + ' avviato sulla porta ' + str(port))
    app.run(host='0.0.0.0', port=port)

