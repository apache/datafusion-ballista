# Graphite

Graphite is a high-performance, asynchronous financial terminal application running natively in the command-line interface (CLI). Built with Rust, it provides real-time tracking of Indian stock market data, specifically focusing on NIFTY 500 companies. The application leverages the efficiency of terminal user interfaces (TUI) to deliver low-latency market visualization and portfolio management.

## Architecture

The application is architected around an asynchronous event-driven model using the Tokio runtime. It separates the user interface rendering from data acquisition to ensure a responsive experience even during network operations.

### Core Components

*   **Application State (`App`)**: The central struct managing the application's lifecycle, including selected company, market data cache, view modes (watchlist, portfolio), and user input state.
*   **Event Handling**: Input events (keyboard) and system events (ticks, resize) are captured via `crossterm` and processed in a dedicated event loop.
*   **Concurrency Model**:
    *   **UI Thread**: The main thread runs the TUI rendering loop, drawing the interface frame-by-frame using `ratatui`.
    *   **Data Fetching**: Network requests to the Yahoo Finance API are offloaded to background `tokio` tasks. This non-blocking design allows the UI to remain interactive while data is retrieved.
    *   **Communication**: `tokio::sync::mpsc` (Multi-Producer, Single-Consumer) unbounded channels are used to pass asynchronous data updates from background workers back to the main application thread.

### Technical Stack

*   **Language**: Rust (2021 Edition)
*   **Async Runtime**: `tokio`
*   **TUI Framework**: `ratatui`
*   **Terminal Backend**: `crossterm`
*   **Data Provider**: `yahoo_finance_api`
*   **Serialization**: `serde` / `serde_json`

## Features

*   **Real-time Market Data**: Fetches and displays live price, volume, and intraday/historical charts for NSE (National Stock Exchange) listed companies.
*   **Interactive Charting**: Line charts rendering historical price movements across multiple timeframes (1 Day, 5 Days, 1 Month, 3 Months).
*   **Watchlist Management**: Persistent watchlist storage allowing users to track specific subsets of stocks.
*   **Portfolio Tracking**: Capability to track holdings, calculating current value, total cost, and unrealized P&L locally.
*   **Search & Filtering**: Efficient fuzzy search implementation to quickly navigate the NIFTY 500 universe.

## Installation and Build

Ensure you have the Rust toolchain (cargo, rustc) installed.

1.  Clone the repository.
2.  Build the project in release mode for optimal performance:

```bash
cargo build --release
```

3.  Run the binary:

```bash
./target/release/graphite
```

### Dependencies

The project relies on `nifty500.csv` being present in the working directory for initializing the company list. If not found, it falls back to a minimal default list.

## Configuration

Graphite stores user data in the home directory under `.graphite/`:

*   `~/.graphite/watchlist.json`: Stores the list of watchlisted symbols.
*   `~/.graphite/portfolio.json`: Stores portfolio holdings (quantity and average price).

## internal Usage

### Navigation

*   `Up` / `k`: Select previous company.
*   `Down` / `j`: Select next company.
*   `/`: Enter search mode.

### Views & Data

*   `1` - `4`: Switch time ranges (1D, 5D, 1M, 3M).
*   `w`: Toggle current stock in watchlist.
*   `W`: Toggle watchlist-only view mode.
*   `p`: Toggle portfolio view.
*   `r`: Force refresh of data.

### Application Control

*   `q` / `Esc`: Quit the application.
*   `?` / `h`: Toggle help overlay.
