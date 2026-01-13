import requests
import csv
import time

# CONFIG 
API_URL = "https://pokeapi.co/api/v2/pokemon/"
TOTAL_POKEMON = 10
OUTPUT_FILE = "pokemon_data.csv"

# EXTRACT 
def extract_pokemon(pokemon_id):
    try:
        response = requests.get(API_URL + str(pokemon_id), timeout=5)

        # Handle rate limiting
        if response.status_code == 429:
            print("Rate limit reached. Waiting...")
            time.sleep(2)
            return extract_pokemon(pokemon_id)

        # Handle API failure
        if response.status_code != 200:
            print(f"API failed for ID {pokemon_id}")
            return None

        return response.json()

    except requests.exceptions.RequestException as e:
        print("API Error:", e)
        return None

# TRANSFORM 
def transform_pokemon(data):
    try:
        pokemon = {
            "id": data["id"],
            "name": data["name"],
            "height": data["height"],
            "weight": data["weight"],
            "types": ", ".join([t["type"]["name"] for t in data["types"]]),
            "abilities": ", ".join([a["ability"]["name"] for a in data["abilities"]])
        }
        return pokemon

    except KeyError:
        print("Invalid response structure")
        return None

# LOAD 
def load_to_csv(pokemon_list):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=pokemon_list[0].keys())
        writer.writeheader()
        writer.writerows(pokemon_list)

# PIPELINE 
def run_etl_pipeline():
    final_data = []

    for i in range(1, TOTAL_POKEMON + 1):
        print(f"Fetching Pokémon {i}")

        raw_data = extract_pokemon(i)
        if raw_data:
            clean_data = transform_pokemon(raw_data)
            if clean_data:
                final_data.append(clean_data)

        time.sleep(1)  # safe for rate limit

    if final_data:
        load_to_csv(final_data)
        print("ETL Pipeline completed successfully!")


# MAIN 
if __name__ == "__main__":
    run_etl_pipeline()


"""import requests
import csv
import time

API_URL = "https://pokeapi.co/api/v2/pokemon/"
TOTAL_POKEMON = 10
OUTPUT_FILE = "pokemon_data.csv"

# ---------------- EXTRACT ----------------
def extract_pokemon(pokemon_id):
    try:
        response = requests.get(f"{API_URL}{pokemon_id}", timeout=5)

        if response.status_code == 429:
            time.sleep(2)
            return extract_pokemon(pokemon_id)

        if response.status_code != 200:
            return None

        return response.json()

    except requests.exceptions.RequestException:
        return None

# ---------------- TRANSFORM ----------------
def transform_pokemon(data):
    try:
        return {
            "id": data["id"],
            "name": data["name"],
            "height": data["height"],
            "weight": data["weight"],
            "types": ", ".join(t["type"]["name"] for t in data["types"]),
            "abilities": ", ".join(a["ability"]["name"] for a in data["abilities"])
        }
    except KeyError:
        return None

# ---------------- LOAD ----------------
def load_to_csv(data):
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

# ---------------- PIPELINE ----------------
def run_etl():
    pokemon_data = []

    for i in range(1, TOTAL_POKEMON + 1):
        raw = extract_pokemon(i)

        if raw:
            clean = transform_pokemon(raw)
            if clean:
                pokemon_data.append(clean)

        time.sleep(1)

    if pokemon_data:
        load_to_csv(pokemon_data)

# ---------------- MAIN ----------------
run_etl()
"""