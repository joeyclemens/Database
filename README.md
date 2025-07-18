# Database (Rust + Axum)

A web-based database management tool built with Rust, Axum, and ODBC. This project provides a modern web interface for browsing, editing, and managing SQL Server (and compatible) databases, with a focus on hierarchical area/room/item scheduling data.

## Features

- **Web UI**: Select and manage databases from a browser.
- **Hierarchical Data Management**: View and edit areas, rooms, and item schedules.
- **ODBC Connection Pooling**: Efficient, concurrent access to multiple databases.
- **RESTful API**: Endpoints for CRUD operations on various database tables.
- **Modern Rust Stack**: Built with Axum, Tokio, Serde, and more.

## How It Works

- The backend is a Rust web server using [Axum](https://github.com/tokio-rs/axum).
- ODBC is used for database connectivity, supporting SQL Server and other ODBC-compatible databases.
- The frontend (see `static/index.html`) allows users to select a database and navigate to management pages.
- The server exposes REST endpoints for all major data operations (view, add, update, delete) on tables like `AreasLevel1`, `AreasLevel2`, `AreasLevel3`, `Room_Schedule`, `Item_Schedule`, etc.

## Project Structure

- `src/main.rs`: Main server code, all routes and handlers.
- `static/`: HTML files for the web UI.
- `Cargo.toml`: Rust dependencies and project metadata.

## Running the Project

1. **Install Rust** (if not already):  
   https://rustup.rs/

2. **Install ODBC driver** for your database (e.g., SQL Server ODBC driver).

3. **Configure your ODBC Data Sources** as needed.

4. **Build and run:**
   ```sh
   cargo run
   ```

5. **Open your browser** to [http://localhost:3000](http://localhost:3000) (or the port shown in the terminal).

## Dependencies

- [axum](https://crates.io/crates/axum)
- [tokio](https://crates.io/crates/tokio)
- [odbc-api](https://crates.io/crates/odbc-api)
- [serde](https://crates.io/crates/serde)
- [bb8](https://crates.io/crates/bb8) (connection pooling)
- [dashmap](https://crates.io/crates/dashmap)
- [webbrowser](https://crates.io/crates/webbrowser)

## Customization

- To add or modify database tables, update the relevant handlers in `main.rs`.
- To change the UI, edit the HTML files in `static/`.

## License

MIT 