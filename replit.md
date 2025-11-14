# File Processing Tools Application

## Overview
This Flask-based web application provides tools for processing and analyzing poker hand histories. It features a TXT/XML File Filter for processing uploaded archives (ZIP/RAR) and a CSV Merger for consolidating data. The application integrates a comprehensive poker hand history parsing module to extract detailed data from hand histories across six major poker sites: PokerStars, GG Poker, Winamax, 888poker, 888.pt, and WPN. The project aims to streamline file management, data analysis, and poker insights through an intuitive, tabbed, drag-and-drop interface, supporting multi-user and large file processing.

## Recent Changes
- **November 14, 2025**: Git pull from GitHub repository completed. System synced with latest remote version. Removed performance-degrading hands_by_stat file generation feature (was causing 3x slower uploads by generating 450+ individual files). Upload performance restored to optimal speed while maintaining full monthly dashboard functionality.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture
The application uses a client-server architecture with a Flask backend (Python 3.11+) and an HTML5/Bootstrap/JavaScript frontend.

### UI/UX Decisions
- Dark theme and responsive design using Bootstrap.
- Drag-and-drop functionality for file uploads.

### Technical Implementations
- **Simplified Upload System**: Synchronous processing architecture with a single worker.
- **Autoscale Production Architecture**: Connection pooling, queue safeguards, memory-efficient streaming, heartbeat & auto-recovery, Replit Object Storage integration, unified storage abstraction, and database optimizations.
- **File Deduplication System**: Uses SHA256 hashing for unique identification and Supabase Storage integration for original files.
- **Supabase Processing History Integration**: Stores all processing history in a Supabase database with metadata and provides API endpoints for querying user history.
- **Performance Optimizations** (November 2025):
    - **Extended Timeout**: Gunicorn worker timeout increased from 300s to 900s (15min) to handle large file uploads
    - **GZIP Compression**: Automatic compression for JSON files >1MB before upload to Supabase Storage (70-90% size reduction)
    - **Smart Download**: Automatic decompression of gzip-compressed files on download with fallback to raw bytes
    - **Progress Logging**: Enhanced logging for large file uploads showing compression ratios and upload progress
    - **Simplified Upload Architecture** (Nov 12, 2025): System now uploads only JSON result files and original ZIP archive to Supabase Storage (no individual TXT files), reducing upload time and storage costs
    - **Cloud-First Dashboard**: Dashboard loads results directly from Supabase Storage via `build_dashboard_payload()` and `ResultStorageService`, eliminating dependency on local `work/` directories after cleanup
- **Poker Hand Parsing Module**: Robust parsing for PokerStars, GGPoker, WPN, Winamax, 888poker, and 888.pt hand histories using Pydantic models, including hero detection, actions, and board cards. Calculates derived statistics and position-specific percentages.
    - **All-in Detection**: Parsers detect `is_allin` before action type.
    - **Centralized Validation System**: `PreflopOpportunityValidator` module centralizes validation logic for stat opportunities, including stat-specific validators with complete stack rules.
    - **Stack Validation**: Validates at the individual stat-opportunity level, including a "at least one opponent ≥16bb" rule.
    - **888poker and 888.pt Specifics**: Corrected European number format handling and implemented 888.pt as a separate poker room with its dedicated parser.
    - **PKO Statistics**: Fixed stack extraction regex to recognize PKO bounty format.
- **Memory Optimization**: Implemented streaming hand processing for large files with garbage collection and memory monitoring.
- **Monthly Data Separation** (November 2025):
    - **Automatic Month-Based Bucketing**: Hand history files are automatically grouped by month before processing using DateExtractor service with site-specific regex patterns for all 6 poker sites
    - **Per-Month Processing**: Each month is processed independently with isolated stats, filters, and aggregations
    - **Months Manifest**: `months_manifest.json` generated with metadata for each month (total_hands, status)
    - **Storage Structure**: Monthly results stored in `results/{token}/months/{YYYY-MM}/pipeline_result.json`
    - **Postflop Aggregation**: Dashboard's `aggregate_postflop_stats()` automatically combines postflop stats from all groups (9max + 6max + PKO) when loading monthly views, ensuring identical functionality to aggregate view
    - **Database Integration**: Added `month` column to `poker_stats_detail` table and `months_summary` JSONB field to `processing_history`
    - **API Endpoints**: 
        - `GET /api/dashboard/<token>/months` - Returns months manifest
        - `GET /api/dashboard/<token>?month=YYYY-MM` - Loads month-specific data
        - `GET /dashboard/<token>?month=YYYY-MM` - Renders dashboard for specific month
    - **UI Month Selector**: Dark-themed dropdown in dashboard header with Portuguese month formatting and hand counts, only visible for multi-month uploads
    - **Backwards Compatible**: Single-month and legacy uploads continue working with aggregate views

### Feature Specifications
- **File Processing**: Supports ZIP and RAR archives with recursive extraction up to 5 levels, using `python-magic` for MIME type detection.
- **Filtering Logic**: Filters TXT and XML files based on poker-related regex patterns, categorizing them into "MYSTERIES," "PKO," and "NON-KO" folders.
- **CSV Merger**: Extracts, combines, and processes data from user-uploaded CSVs, calculating VPIP columns.
- **NON-KO Summary**: Generates `nonko_combined.json` with monthly breakdowns by table format.
- **Partition Validation**: `validator.py` ensures data integrity and generates `validation_report.json`.
- **Hand History Service Module**: Provides indexing and API access to parsed hand histories.
- **Manifest Generation**: `classification_manifest.json` is generated per processing run.
- **UI Display**: All 32 preflop stats are always visible, showing "N/A" for stats without opportunities.

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
-   **Third-party Services**:
    -   `Supabase` (for storage and processing history)