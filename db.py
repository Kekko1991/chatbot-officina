import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL', '')


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    """Crea le tabelle se non esistono."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    phone TEXT PRIMARY KEY,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS conversations (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL REFERENCES customers(phone),
                    messages JSONB DEFAULT '[]',
                    pending_slots JSONB,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            cur.execute('''
                CREATE INDEX IF NOT EXISTS idx_conversations_phone_active
                ON conversations(phone, active) WHERE active = TRUE
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS triage_results (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL REFERENCES customers(phone),
                    priority TEXT,
                    category TEXT,
                    summary TEXT,
                    recommendation TEXT,
                    emotional_note TEXT,
                    raw_json JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS bookings (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL REFERENCES customers(phone),
                    triage_id INTEGER REFERENCES triage_results(id),
                    slot_date DATE NOT NULL,
                    slot_time TEXT NOT NULL,
                    slot_display TEXT,
                    datetime_start TIMESTAMP WITH TIME ZONE,
                    datetime_end TIMESTAMP WITH TIME ZONE,
                    status TEXT DEFAULT 'confermato',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS processed_messages (
                    msg_id TEXT PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            # Colonne aggiunte dopo la creazione iniziale
            for col, tipo in [('nome_cliente', 'TEXT'), ('auto_cliente', 'TEXT'), ('google_event_id', 'TEXT'), ('promemoria_inviato', 'BOOLEAN DEFAULT FALSE')]:
                try:
                    cur.execute('ALTER TABLE bookings ADD COLUMN ' + col + ' ' + tipo)
                except Exception:
                    conn.rollback()
                    conn = get_conn()
                    cur = conn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS feedback (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL REFERENCES customers(phone),
                    booking_id INTEGER REFERENCES bookings(id),
                    rating INTEGER NOT NULL,
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS photos (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL REFERENCES customers(phone),
                    booking_id INTEGER REFERENCES bookings(id),
                    image_data TEXT NOT NULL,
                    media_type TEXT DEFAULT 'image/jpeg',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
        conn.commit()
        logger.info('Database inizializzato con successo')
    except Exception as e:
        logger.error('Errore init DB: ' + str(e))
        conn.rollback()
        raise
    finally:
        conn.close()


# --- CUSTOMERS ---

def upsert_customer(phone, name=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO customers (phone, name)
                VALUES (%s, %s)
                ON CONFLICT (phone) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, customers.name),
                    updated_at = NOW()
            ''', (phone, name))
        conn.commit()
    finally:
        conn.close()


def get_customer_name(phone):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT name FROM customers WHERE phone = %s', (phone,))
            row = cur.fetchone()
            return row['name'] if row else None
    finally:
        conn.close()


# --- DEDUPLICAZIONE ---

def is_duplicate(msg_id):
    if not msg_id:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM processed_messages WHERE msg_id = %s', (msg_id,))
            if cur.fetchone():
                return True
            cur.execute('INSERT INTO processed_messages (msg_id) VALUES (%s)', (msg_id,))
            # Pulizia messaggi vecchi (> 5 minuti)
            cur.execute("DELETE FROM processed_messages WHERE processed_at < NOW() - INTERVAL '5 minutes'")
        conn.commit()
        return False
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()


# --- CONVERSATIONS ---

def get_conversation(phone):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT messages, pending_slots FROM conversations
                WHERE phone = %s AND active = TRUE
                ORDER BY updated_at DESC LIMIT 1
            ''', (phone,))
            row = cur.fetchone()
            if row:
                return row['messages'] or [], row['pending_slots']
            return [], None
    finally:
        conn.close()


def save_conversation(phone, messages, pending_slots=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT id FROM conversations
                WHERE phone = %s AND active = TRUE
                ORDER BY updated_at DESC LIMIT 1
            ''', (phone,))
            row = cur.fetchone()
            if row:
                cur.execute('''
                    UPDATE conversations
                    SET messages = %s, pending_slots = %s, updated_at = NOW()
                    WHERE id = %s
                ''', (json.dumps(messages), json.dumps(pending_slots) if pending_slots else None, row['id']))
            else:
                cur.execute('''
                    INSERT INTO conversations (phone, messages, pending_slots)
                    VALUES (%s, %s, %s)
                ''', (phone, json.dumps(messages), json.dumps(pending_slots) if pending_slots else None))
        conn.commit()
    finally:
        conn.close()


def clear_conversation(phone):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                UPDATE conversations SET active = FALSE, updated_at = NOW()
                WHERE phone = %s AND active = TRUE
            ''', (phone,))
        conn.commit()
    finally:
        conn.close()


# --- TRIAGE ---

def save_triage(phone, triage_data):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO triage_results (phone, priority, category, summary, recommendation, emotional_note, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                phone,
                triage_data.get('priority'),
                triage_data.get('category'),
                triage_data.get('summary'),
                triage_data.get('recommendation'),
                triage_data.get('emotional_note'),
                json.dumps(triage_data),
            ))
            row = cur.fetchone()
        conn.commit()
        return row['id']
    finally:
        conn.close()


def get_latest_triage(phone):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT * FROM triage_results
                WHERE phone = %s
                ORDER BY created_at DESC LIMIT 1
            ''', (phone,))
            return cur.fetchone()
    finally:
        conn.close()


# --- BOOKINGS ---

def create_booking(phone, slot, triage_id=None, nome_cliente=None, auto_cliente=None, google_event_id=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO bookings (phone, triage_id, slot_date, slot_time, slot_display, datetime_start, datetime_end, nome_cliente, auto_cliente, google_event_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                phone, triage_id,
                slot.get('date'), slot.get('time'), slot.get('display'),
                slot.get('datetime_start'), slot.get('datetime_end'),
                nome_cliente, auto_cliente, google_event_id,
            ))
            row = cur.fetchone()
        conn.commit()
        return row['id']
    finally:
        conn.close()


def get_latest_booking_id(phone):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT id FROM bookings
                WHERE phone = %s
                ORDER BY created_at DESC LIMIT 1
            ''', (phone,))
            row = cur.fetchone()
            return row['id'] if row else None
    finally:
        conn.close()


def find_booking_by_name_plate(nome, targa):
    """Cerca una prenotazione attiva per nome cliente e targa (contenuta in auto_cliente)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT b.*, t.priority, t.category, t.summary, t.recommendation
                FROM bookings b
                LEFT JOIN triage_results t ON b.triage_id = t.id
                WHERE LOWER(b.nome_cliente) = LOWER(%s)
                AND LOWER(b.auto_cliente) LIKE LOWER(%s)
                AND b.status = 'confermato'
                ORDER BY b.created_at DESC LIMIT 1
            ''', (nome.strip(), '%' + targa.strip() + '%'))
            return cur.fetchone()
    finally:
        conn.close()


def find_active_booking_by_phone(phone):
    """Cerca la prenotazione attiva piu' recente per numero di telefono."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT b.*, t.priority, t.category, t.summary, t.recommendation
                FROM bookings b
                LEFT JOIN triage_results t ON b.triage_id = t.id
                WHERE b.phone = %s AND b.status = 'confermato'
                ORDER BY b.created_at DESC LIMIT 1
            ''', (phone,))
            return cur.fetchone()
    finally:
        conn.close()


def cancel_booking(booking_id):
    """Cancella una prenotazione (imposta status a 'cancellato')."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE bookings SET status = 'cancellato' WHERE id = %s", (booking_id,))
        conn.commit()
    finally:
        conn.close()


def update_booking(booking_id, new_slot, new_google_event_id=None):
    """Aggiorna una prenotazione con un nuovo slot."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                UPDATE bookings
                SET slot_date = %s, slot_time = %s, slot_display = %s,
                    datetime_start = %s, datetime_end = %s, google_event_id = %s
                WHERE id = %s
            ''', (
                new_slot.get('date'), new_slot.get('time'), new_slot.get('display'),
                new_slot.get('datetime_start'), new_slot.get('datetime_end'),
                new_google_event_id, booking_id,
            ))
        conn.commit()
    finally:
        conn.close()


def get_booking_google_event_id(booking_id):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT google_event_id FROM bookings WHERE id = %s', (booking_id,))
            row = cur.fetchone()
            return row['google_event_id'] if row else None
    finally:
        conn.close()


def save_feedback(phone, booking_id, rating, comment=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO feedback (phone, booking_id, rating, comment)
                VALUES (%s, %s, %s, %s)
            ''', (phone, booking_id, rating, comment))
        conn.commit()
    finally:
        conn.close()


def save_photo(phone, image_data, media_type='image/jpeg', booking_id=None):
    """Salva una foto inviata dal cliente."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO photos (phone, booking_id, image_data, media_type)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            ''', (phone, booking_id, image_data, media_type))
            row = cur.fetchone()
        conn.commit()
        return row['id']
    finally:
        conn.close()


def link_photos_to_booking(phone, booking_id):
    """Collega le foto non associate a un booking appena creato."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                UPDATE photos SET booking_id = %s
                WHERE phone = %s AND booking_id IS NULL
            ''', (booking_id, phone))
        conn.commit()
    finally:
        conn.close()


def get_photos_for_booking(booking_id):
    """Ritorna le foto associate a una prenotazione."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT id, media_type, created_at FROM photos
                WHERE booking_id = %s
                ORDER BY created_at ASC
            ''', (booking_id,))
            return cur.fetchall()
    finally:
        conn.close()


def get_photo_data(photo_id):
    """Ritorna i dati di una singola foto."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT image_data, media_type FROM photos WHERE id = %s', (photo_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_tomorrow_bookings():
    """Ritorna le prenotazioni confermate per domani che non hanno ancora ricevuto il promemoria."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT id, phone, nome_cliente, auto_cliente, slot_time, slot_display
                FROM bookings
                WHERE status = 'confermato'
                AND slot_date = CURRENT_DATE + INTERVAL '1 day'
                AND (promemoria_inviato IS NULL OR promemoria_inviato = FALSE)
            ''')
            return cur.fetchall()
    finally:
        conn.close()


def mark_promemoria_sent(booking_id):
    """Segna che il promemoria è stato inviato per questa prenotazione."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('UPDATE bookings SET promemoria_inviato = TRUE WHERE id = %s', (booking_id,))
        conn.commit()
    finally:
        conn.close()


def count_bookings_by_week(date_start, date_end):
    """Conta le prenotazioni confermate per settimana (lun-dom) nel periodo."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT date_trunc('week', slot_date)::date as week_start, COUNT(*) as total
                FROM bookings
                WHERE status = 'confermato'
                AND slot_date >= %s AND slot_date <= %s
                GROUP BY week_start
            ''', (date_start, date_end))
            rows = cur.fetchall()
            return {str(r['week_start']): r['total'] for r in rows}
    finally:
        conn.close()


def get_active_bookings():
    """Ritorna tutte le prenotazioni confermate con data futura."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT b.id, b.phone, b.nome_cliente, b.auto_cliente,
                       b.slot_date, b.slot_time, b.slot_display,
                       c.name as customer_name,
                       t.summary, t.priority, t.category
                FROM bookings b
                JOIN customers c ON b.phone = c.phone
                LEFT JOIN triage_results t ON b.triage_id = t.id
                WHERE b.status = 'confermato'
                AND b.slot_date >= CURRENT_DATE
                ORDER BY b.slot_date ASC, b.slot_time ASC
            ''')
            return cur.fetchall()
    finally:
        conn.close()


def get_bookings(phone=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if phone:
                cur.execute('''
                    SELECT b.*, c.name as customer_name, t.summary, t.priority, t.category
                    FROM bookings b
                    JOIN customers c ON b.phone = c.phone
                    LEFT JOIN triage_results t ON b.triage_id = t.id
                    WHERE b.phone = %s
                    ORDER BY b.created_at DESC
                ''', (phone,))
            else:
                cur.execute('''
                    SELECT b.*, c.name as customer_name, t.summary, t.priority, t.category
                    FROM bookings b
                    JOIN customers c ON b.phone = c.phone
                    LEFT JOIN triage_results t ON b.triage_id = t.id
                    ORDER BY b.created_at DESC
                ''')
            return cur.fetchall()
    finally:
        conn.close()
