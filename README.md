ETL Pipeline Project

This project implements an ETL pipeline using Python.

Extract:
- Fetches data from Pokémon public API

Transform:
- Normalizes nested JSON (types, abilities)

Load:
- Stores cleaned data into CSV file

Error Handling:
- API failures
- Rate limiting
- Invalid responses

## How to Run

1. Clone the repository
git clone https://github.com/Garvit0606/Pokemon-etl-pipeline.git
cd Pokemon-etl-pipeline

2. Install dependencies
pip install -r requirements.txt


3.Run the ETL pipeline
python main.py


4.Output will be saved as:
pokemon_data.csv

