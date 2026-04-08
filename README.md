# 🌍 GDP ETL Project

## 📌 Project Overview

This project is an **ETL (Extract, Transform, Load) pipeline** developed in Python. It extracts GDP data of countries from a Wikipedia page (archived via Wayback Machine), processes it, and stores it in both CSV and SQLite database formats.

The goal is to automate the retrieval of global GDP data as published by the **International Monetary Fund (IMF)** and make it easily accessible for analysis.

---

## 🎯 Objectives

- Extract country-wise GDP data from a web source
- Transform GDP values from **Million USD to Billion USD**
- Store processed data into:
  - A CSV file
  - A SQLite database

- Query the database for countries with GDP ≥ 100 billion USD
- Log each stage of the ETL process with timestamps

---

## 🛠️ Technologies Used

- Python
- Pandas
- BeautifulSoup (bs4)
- Requests
- SQLite3
- Regular Expressions (re)

---

## 📂 Project Structure

```
├── etl_project_gdp.py       # Main ETL script
├── Countries_by_GDP.csv     # Output CSV file
├── World_Economies.db       # SQLite database
├── etl_log.txt              # Log file
└── README.md                # Project documentation
```

---

## ⚙️ How It Works

### 1. Extract

- Fetches HTML data from the archived Wikipedia page
- Parses the GDP table using BeautifulSoup
- Cleans country names and GDP values

### 2. Transform

- Removes unwanted characters and references
- Converts GDP from string to numeric
- Transforms GDP from **Millions to Billions**
- Rounds values to **2 decimal places**

### 3. Load

- Saves data into:
  - `Countries_by_GDP.csv`
  - SQLite database (`World_Economies.db`)

- Creates a table:

  ```
  Countries_by_GDP(Country, GDP_USD_billion)
  ```

### 4. Query

- Executes:

  ```sql
  SELECT *
  FROM Countries_by_GDP
  WHERE GDP_USD_billion >= 100;
  ```

### 5. Logging

- Logs all ETL stages with timestamps into:

  ```
  etl_log.txt
  ```

---

## 🚀 How to Run

1. Install required libraries:

```bash
pip install pandas requests beautifulsoup4
```

2. Run the script:

```bash
python etl_project_gdp.py
```

---

## 📊 Output

- CSV file containing all countries and GDP values
- SQLite database with the same dataset
- Query results printed to the console
- Log file tracking execution progress

---

## 🧪 Example Log Entry

```
2026-04-08-14:30:12,ETL Job Started
2026-04-08-14:30:15,Extract phase Started
...
2026-04-08-14:30:25,ETL Job Completed
```

---

## ⚠️ Notes

- The script uses an archived webpage to ensure consistent data structure
- GDP values are approximate due to rounding
- Internet connection is required for extraction

---

## 👨‍💻 Author

Junior Data Engineer – ETL Project

---

## 📄 License

This project is for educational purposes.
