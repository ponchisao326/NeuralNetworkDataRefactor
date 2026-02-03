# Pixelmon AI Data Pipeline

## Project Description

Pixelmon AI Data Pipeline is a Data Engineering solution designed to transform raw event logs (Pixelmon Minecraft server logs) into structured, clean datasets optimized for Neural Network training and advanced statistical analysis.

The project refactors legacy monolithic scripts into a modular Object-Oriented architecture, implementing robust design patterns to ensure **reproducibility**, **data integrity**, and **scalability**.

---

## Architecture and Design

The core of the system relies on the **Template Method Design Pattern**. This standardizes the ETL (Extract, Transform, Load) workflow while maintaining the flexibility required to process different game event types.

### Pipeline Workflow
Every event type (Battles, Captures, Economy, etc.) strictly follows the lifecycle defined in the `BaseDataPipeline` abstract class:

1.  **Extraction (`_extract_data`):**
    * Downloads data from the external API.
    * Implements a local caching system (CSVs in `data/raw`) to prevent redundant API calls and ensure consistency during development.
2.  **Context Cleaning (`_clean_json_context`):**
    * Normalizes columns containing nested JSON or Python dictionary representations (single-quoted strings).
    * Uses a hybrid parsing strategy (`json.loads` with a fallback to `ast.literal_eval`) to handle formatting errors robustly.
3.  **Feature Engineering (`_feature_engineering`):**
    * Applies specific business logic (e.g., IV percentage calculation, session duration, death categorization).
    * Converts data types and handles null values.
4.  **Standard Encoding:**
    * Applies automatic One-Hot Encoding to common categorical variables such as `server_id`.
5.  **Persistence (`_save_data`):**
    * Saves processed datasets to `data/clean/`, ready for consumption by ML models.
6.  **Reporting (`_generate_visualization_data`):**
    * Generates interactive charts using **Plotly** and injects them into a consolidated HTML report.

---

## Project Structure

```text
pixelmon_ai_project/
├── data/
│   ├── raw/            # Raw data downloaded from the API (Cache)
│   ├── clean/          # Processed datasets ready for ML
│   └── reports/        # Generated HTML reports
├── src/
│   ├── connectors/     # Singleton API Client
│   ├── pipelines/      # ETL Business Logic
│   │   ├── base_pipeline.py  # Abstract Class (Template Method)
│   │   ├── battles.py        # Battles Logic
│   │   ├── breeding.py       # Breeding Logic
│   │   ├── captures.py       # Captures Logic
│   │   ├── economy.py        # Economy Logic (GTS)
│   │   └── ... (other pipelines)
│   ├── reporting/      # HTML Report Generation Engine
│   │   ├── templates/  # Jinja2 Templates
│   │   └── html_generator.py
│   ├── config.py       # Environment Variable Management
│   └── main.py         # Main Orchestrator
├── notebooks/          # (Future) Notebooks for AI training
├── .env                # Credentials and Configuration (Not included in repo)
├── requirements.txt    # Project Dependencies
└── README.md           # Documentation

```

---

## Installation and Usage

### Prerequisites

* Python 3.10+
* Internet access for the initial data download.

### 1. Clone the Repository

```bash
git clone https://github.com/ponchisao326/NeuralNetworkDataRefactor.git
cd pixelmon-ai-data
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root based on the following schema:

```ini
API_URL=https://your-api-endpoint.com/v1/events
API_KEY=your_secret_key_here
RAW_DATA_DIR=data/raw
CLEAN_DATA_DIR=data/clean
REPORT_DIR=data/reports
```

### 5. Run the Pipeline

Execute the main orchestrator. This script will instantiate all pipelines, process the data, and generate the report.

```bash
python src/main.py
```

---

## Visualization and Reporting

The system automatically generates an interactive report at:
`data/reports/ai_training_report.html`

This report includes key metrics such as:

* **Economy:** Top sales, price/level correlation, volume per server.
* **Gameplay:** Session duration, activity heatmaps, deadliest biomes.
* **Mechanics:** Genetic quality (IVs) of captured vs. released/bred Pokémon.

**Technical Note on Visualization:**
The report uses **Plotly** decoupled from Pandas. Data is serialized into native Python lists before being injected into the HTML to ensure full compatibility with modern browsers and avoid NumPy type conflicts.

---

## Technologies Used

* **Language:** Python 3.11
* **Data Manipulation:** Pandas, NumPy
* **Visualization:** Plotly Graph Objects (No Express, for robustness)
* **Reporting:** Jinja2 (HTML Templating)
* **Connectivity:** Requests
* **Configuration Management:** Python-dotenv

---

## Next Steps

The next project milestone is to use the data stored in `data/clean/` to train predictive models in the `notebooks/ai_training/` directory:

1. Market price prediction (Regression).
2. Battle win prediction (Classification).
3. Player behavior anomaly detection.

---

**Author:** Víctor Gómez Ponce
**Version:** 2.0.0 (OOP Refactor)
