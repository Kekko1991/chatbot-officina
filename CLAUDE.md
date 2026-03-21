# Chatbot Officina

WhatsApp chatbot for an Italian auto repair shop ("officina"). Built with Flask + Claude API + Meta WhatsApp Business API, with optional Google Calendar integration.

## Architecture

Single-file Flask app (`app.py`) deployed on Render via gunicorn (`Procfile`). All state (conversations, triage data, message dedup, user locks) is in-memory — no database.

### Flow
1. Customer sends WhatsApp message → Meta webhook → Flask `/webhook` POST
2. Message processed in a background thread with per-user locking
3. Claude (Sonnet) performs triage via conversation (2-3 exchanges), then returns structured JSON
4. Bot formats triage result with priority, shows available appointment slots
5. Customer picks a slot → event optionally created in Google Calendar

## Commands

- **Run locally:** `python app.py` (starts on port 5000)
- **Production:** `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`
- **Dependencies:** `pip install -r requirements.txt`

## Environment Variables

All config via env vars (see `app.py` lines 15-30): `ANTHROPIC_API_KEY`, `META_ACCESS_TOKEN`, `META_PHONE_NUMBER_ID`, `META_VERIFY_TOKEN`, dealership info (`NOME_CONCESSIONARIO`, `INDIRIZZO`, `TELEFONO_OFFICINA`), hours (`ORARIO_APERTURA`, `ORARIO_CHIUSURA`, etc.), `GOOGLE_CREDENTIALS_JSON`, `GOOGLE_CALENDAR_ID`.

## Style

- Language: Italian for user-facing strings, code variables in Italian
- No tests or linting configured
- String concatenation used throughout (no f-strings)
