# File Processing Tools Application

## Overview
This Flask-based web application provides tools for processing and analyzing poker hand histories. It features a TXT/XML File Filter for processing uploaded archives (ZIP/RAR) and a CSV Merger for consolidating data. The application integrates a comprehensive poker hand history parsing module to extract detailed data from hand histories across six major poker sites: PokerStars, GG Poker, Winamax, 888poker, 888.pt, and WPN. The project aims to streamline file management, data analysis, and poker insights through an intuitive, tabbed, drag-and-drop interface, supporting multi-user and large file processing.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture
The application uses a client-server architecture with a Flask backend (Python 3.11+) and an HTML5/Bootstrap/JavaScript frontend.

### UI/UX Decisions
- Dark theme and responsive design using Bootstrap.
- Drag-and-drop functionality for file uploads.
- All 32 preflop stats are always visible, even if opportunities are 0, displaying "N/A" for missing data.

### Technical Implementations
- **Simplified Upload System**: Synchronous processing with a single worker, direct upload to `/tmp`, and immediate processing.
- **Autoscale Production Architecture**: Features connection pooling, queue safeguards with rate limits and `SERIALIZABLE` transactions, memory-efficient streaming for large files, heartbeat and auto-recovery for workers, Replit Object Storage integration with a unified storage abstraction (Object Storage in production, local filesystem in development), and database optimizations using compound indexes and `SELECT FOR UPDATE SKIP LOCKED`. All pipeline results are uploaded to Object Storage.
- **Poker Hand Parsing Module**: Robust parsing for six major poker sites using Pydantic models. Extracts detailed information, including hero detection, actions, and board cards. Calculates derived statistics (e.g., RFI, ISO, 3-bet, 4-bet, squeeze, BvB) and position-specific percentages. Includes all-in detection.
- **Unified Validation System**: Centralized `ValidatorBase` class with shared validation logic. All 52 statistics (32 preflop + 20 postflop) enforce ≥16bb stack validation for hero and opponents plus all-in detection before hero acts. `PreflopOpportunityValidator` and `PostflopOpportunityValidator` inherit from base with stat-specific rules.
- **Monthly Separation System**: Complete monthly breakdown of statistics with filtering capabilities. All hands are grouped by month during parsing using `extract_month_from_hand()` which supports all 6 poker sites including European date formats (DD-MM-YYYY). Dashboard provides dropdown selector "Visão" to filter stats and downloads by individual month or view aggregate totals.
- **Memory Optimization**: Implemented streaming hand processing for large multi-site mixed files, with garbage collection every 100 hands and increased worker timeout (300s).
- **European Number Format Handling**: Corrected `parse_amount()` to correctly interpret European number formats (dots as thousands separators) for sites like 888poker and 888.pt.
- **PKO Statistics Support**: Updated stack extraction regex to recognize PKO bounty formats.

### Feature Specifications

#### Statistics & Analysis
- **52 Validated Statistics**: All 32 preflop + 20 postflop stats enforce ≥16bb stack validation and all-in detection.
  - **Preflop Stats (32)**: RFI (Early/Middle/CO/BTN/SB), 3-bet positions, Cold Call, Squeeze, BvB (SB Steal, BB Defense), Fold to 3bet (IP/OOP).
  - **Postflop Stats (20)**: Flop CBet/Fold to CBet (IP/OOP), Turn CBet/Fold (IP/OOP), River CBet/Fold (IP/OOP), Flop Raise/Fold to Raise, Turn Raise/Fold to Raise, River Raise/Fold to Raise.
- **Monthly Breakdown**: Complete time-series analysis with per-month statistics and aggregate views.
  - Hands automatically grouped by month during parsing.
  - API endpoint `/api/dashboard/<token>?include_months=true` returns both aggregate and monthly data.
  - UI dropdown "Visão" allows filtering by individual month or viewing all data aggregated.
  - Deep linking support: `/dashboard/<token>?month=2025-11` loads November 2025 directly.
  - Downloads respect selected month: `/api/download/hands_by_stat/<token>/<format>/<stat>?month=2025-11`.
- **Validation System**: `ValidatorBase` class provides centralized validation logic.
  - Universal ≥16bb stack rule for hero and all opponents.
  - All-in detection ensures no premature action invalidates opportunity.
  - Stat-specific validators in `PreflopOpportunityValidator` and `PostflopOpportunityValidator`.
- **Parity Testing**: Automated validation script (`app/tests/test_monthly_parity.py`) ensures sum of monthly stats equals aggregate totals.

#### File Processing
- **File Processing**: Supports ZIP and RAR archives with recursive extraction up to 5 levels, using `python-magic` for MIME type detection.
- **Filtering Logic**: Filters TXT and XML files based on poker-related regex patterns, categorizing them into "MYSTERIES," "PKO," and "NON-KO" folders.
- **CSV Merger**: Extracts, combines, and processes data from user-uploaded CSVs, calculating VPIP columns.
- **NON-KO Summary**: Generates `nonko_combined.json` with monthly breakdowns by table format.
- **Partition Validation**: `validator.py` ensures data integrity and generates `validation_report.json`.
- **Hand History Service Module**: Provides indexing and API access to parsed hand histories.
- **Manifest Generation**: `classification_manifest.json` is generated per processing run.

### Data Storage Architecture
- **Directory Structure**:
  ```
  /results/{token}/
    ├── pipeline_result.json          # Contains aggregate + months_data
    ├── hands_by_stat/                # Aggregate hand files
    │   ├── nonko_9max/
    │   ├── nonko_6max/
    │   └── pko/
    └── months/                       # Monthly breakdown
        └── {YYYY-MM}/
            ├── pipeline_result.json  # Monthly stats only
            └── hands_by_stat/        # Monthly hand files
                ├── nonko_9max/
                ├── nonko_6max/
                └── pko/
  ```
- **API Endpoints**:
  - `/api/dashboard/<token>?include_months=true` - Returns aggregate + all monthly data
  - `/api/dashboard/<token>?month=YYYY-MM` - Returns specific month data
  - `/api/hands_by_stat/<token>?month=YYYY-MM` - Lists monthly hand files
  - `/api/download/hands_by_stat/<token>/<format>/<stat>?month=YYYY-MM` - Downloads monthly hands
- **Storage Backend**: Unified storage abstraction layer supports Object Storage (production) and local filesystem (development).

### System Design Choices
- **Deployment**: Configured for Replit deployment using Gunicorn.
- **Data Synchronization and Backup**: Uses Alembic for database migrations, backup/restore scripts, and production-to-development sync.

## External Dependencies

-   **Python Packages**:
    -   `Flask`
    -   `python-magic`
    -   `rarfile`
    -   `gunicorn`
    -   `flask-sqlalchemy`
    -   `psycopg2-binary`
    -   `email-validator`
    -   `alembic`
-   **System Dependencies**:
    -   `unrar`
    -   `PostgreSQL`
    -   `OpenSSL`
    -   `libarchive`