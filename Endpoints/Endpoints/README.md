# UK Property Scraper Pipeline

Production-grade multi-scraper system for UK property data intelligence. Collects, processes, and syncs property data from multiple sources into a centralized database.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 FastAPI (app.py)                 │
│          REST API + Swagger UI (/docs)           │
├──────────┬──────────┬──────────┬────────────────┤
│ Rightmove│ EPC      │ EPC Deep │ Image          │
│ Scraper  │ Simple   │ Scraper  │ Scraper        │
│          │ Scraper  │          │                │
├──────────┴──────────┴──────────┴────────────────┤
│            Supabase (PostgreSQL)                 │
│            + SQL Server (EPC)                    │
└─────────────────────────────────────────────────┘
```

## Scrapers

### 1. Rightmove Scraper (`rightmovescraper/`)
Scrapes UK's largest property portal for residential listings.

**Capabilities:**
- Async scraping engine — 750+ properties in ~2 minutes
- 30+ data points per property (price, address, agent, EPC, price history, etc.)
- Smart upsert — detects new vs existing, tracks price/status changes
- Status lifecycle — For Sale → Sold STC → Under Offer → Sold → Removed
- Cleanup & reconciliation — marks delisted properties as Removed
- Rate-limit handling with staggered requests and automatic retry
- Duplicate detection for featured listings across pages

### 2. EPC Simple Scraper (`Simplescraper/`)
Scrapes the UK Government's Energy Performance Certificate register.

**Capabilities:**
- Change detection engine (count, dates, URNs per postcode)
- Incremental updates — only processes changed postcodes
- Certificate replacement detection (same address, new URN)
- Proxy rotation for large-scale operations
- CSV-based tracker for state management

### 3. EPC Deep Scraper (`epc_deep_scraper/`)
Deep scrapes individual EPC certificate pages for detailed energy data.

**Capabilities:**
- Async scraping with configurable concurrency
- Supabase database integration
- Proxy support

### 4. Image Scraper (`image_scraper/`)
Extracts and processes EPC images from property listings.

**Capabilities:**
- OpenAI Vision API integration for image analysis
- Automated EPC rating extraction

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/RightmoveScraper` | Upload CSV and trigger Rightmove scrape |
| POST | `/EpcSimpleScraper-trigger/` | Trigger EPC scrape |
| GET | `/EpcSimpleScraper-job-status/` | Check job progress |
| GET | `/EpcSimpleScraper-scraper-status/` | Get scraper state |
| GET | `/EpcSimpleScraper-list-files/` | List postcode CSV files |
| POST | `/EpcSimpleScraper_upload/` | Upload postcode CSVs |

## Tech Stack

- **Backend:** Python, FastAPI, aiohttp, asyncio
- **Scraping:** BeautifulSoup, requests
- **Database:** Supabase (PostgreSQL), SQL Server
- **AI/ML:** OpenAI Vision API (image analysis)
- **Infrastructure:** AWS EC2, Nginx, Systemd
- **API:** RESTful with Swagger UI

## Setup

### 1. Clone and install dependencies

```bash
pip install -r rightmovescraper/reuirements.txt
pip install -r Simplescraper/requirements.txt
pip install -r epc_deep_scraper/requirements.txt
pip install -r image_scraper/requirements.txt
```

### 2. Configure environment variables

Create a `.env` file in the root directory:

```env
# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_service_key

# SQL Server (for EPC scraper)
SERVER=your_server_ip
DATABASE=your_database
USER_NAME=your_username
PASSWORD=your_password

# OpenAI (for image scraper)
OPENAI_API_KEY=your_openai_key

# Optional
BULK_INSERT_API_KEY=your_bulk_api_key
RIGHTMOVE_USE_ASYNC=1
RIGHTMOVE_SCRAPER_CONCURRENCY=24
RIGHTMOVE_PAGE_CONCURRENCY=8
```

### 3. Run the API server

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

### 4. Access Swagger UI

Open `http://localhost:8000/docs` in your browser.

## Project Structure

```
├── app.py                          # FastAPI main application
├── Simplescraper/                  # EPC Simple Scraper
│   ├── main.py                     # Entry point
│   ├── scraper.py                  # Core scraping + change detection
│   ├── uploader.py                 # Database upload logic
│   ├── db.py                       # SQL Server connection
│   └── ...
├── rightmovescraper/               # Rightmove Property Scraper
│   ├── src/
│   │   ├── main.py                 # Entry point
│   │   ├── parser.py               # HTML parsing + async engine
│   │   ├── handlers.py             # Postcode processing + cleanup
│   │   ├── appendindb.py           # Database upsert logic
│   │   └── ...
│   └── db/db.py                    # Supabase REST client
├── epc_deep_scraper/               # EPC Deep Scraper
│   ├── src/
│   │   ├── main.py
│   │   ├── scraper.py
│   │   └── scraper_async.py
│   └── database/db.py
└── image_scraper/                  # EPC Image Processor
    ├── scripts/main.py
    ├── extractor/image_processor.py
    └── database/db_connector.py
```

## Deployment

Deployed on **AWS EC2** (Ubuntu) with:
- Nginx reverse proxy
- Systemd service for auto-restart
- Elastic IP for permanent address

```bash
# Deploy updated files
scp -i key.pem file.py ubuntu@SERVER_IP:/path/to/app/

# Restart service
ssh -i key.pem ubuntu@SERVER_IP "sudo systemctl restart fastapi_app"
```
