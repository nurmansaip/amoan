# amoCRM Manager Analytics

Flask dashboard for amoCRM manager efficiency analytics.

## Railway variables

Set these variables in Railway:

```env
CLIENT_ID=
CLIENT_SECRET=
REDIRECT_URI=https://example.com
SUBDOMAIN=invictusfitness
ACCESS_TOKEN=
REFRESH_TOKEN=
TOKENS_PATH=/data/tokens.json
CACHE_PATH=/data/dashboard_cache.json
```

Use a Railway volume mounted to `/data` so refreshed amoCRM tokens and dashboard cache survive restarts.

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python app.py
```
