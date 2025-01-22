# get_aircraft.py

import requests
import datetime  # Use the whole datetime module instead of from datetime import datetime
from zoneinfo import ZoneInfo
import time
import json
import os
from collections import defaultdict
import getpass
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log
import logging
import csv
from urllib.parse import urlencode

# ============================
# ========= CONFIG ===========
# ============================

MAX_RETRIES = 20  # Max number of retry attempts for API
WAIT_MULTIPLIER = 1  # Base wait time in seconds
WAIT_MAX = 5  # Max wait time between retries

SAVE_UNIQUE_MODELS_TYPECODES = False
SAVE_HOURLY_TIMESERIES = True

# If you store callsign->country in your local CSV, set this to True.
# If not, we always rely on the ADSBDB API for callsign lookups.
LOCAL_CALLSIGN_COUNTRY_ENABLED = True

# Setup logging for retries
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================
# === CSV LOOKUP MODULE  ====
# ============================

import csv_lookup  # your module: loads icao24 -> {country, model, typecode}

# If you store callsign -> country in a local CSV, define a small loader:
def load_callsign_country(csv_path="callsign_to_country.csv"):
    """
    Example CSV columns: callsign,country
      RPA4585,United States
      UAE440,United Arab Emirates
      ...
    Returns a dict: callsign_map[callsign_upper] = "Country Name"
    """
    mapping = {}
    if not os.path.exists(csv_path):
        print(f"Warning: The callsign-country CSV '{csv_path}' is missing.")
        return mapping

    try:
        with open(csv_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                csign = row.get('callsign', '').strip().upper()
                country = row.get('country', '').strip()
                if csign:
                    mapping[csign] = country
        print(f"Loaded {len(mapping)} callsign -> country entries from '{csv_path}'.")
    except Exception as e:
        print(f"Error loading callsign->country CSV: {e}")

    return mapping

def get_country_from_adsbdb(callsign):
    """
    Calls https://api.adsbdb.com/v0/callsign/<CALLSIGN> to retrieve flight origin country.
    Example response snippet:
      {
        "response": {
          "flightroute": {
            "callsign": "RPA4585",
            "airline": {
              "name": "Republic Airlines",
              "country": "United States",
              ...
            },
            "origin": {
              "country_name": "United States",
              ...
            }
          }
        }
      }
    We'll prefer 'origin.country_name' if available. Fallback to 'airline.country'.
    """
    if not callsign:
        return None  # no callsign

    callsign_str = callsign.strip().upper().replace(" ", "")
    url = f"https://api.adsbdb.com/v0/callsign/{callsign_str}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        # try origin first
        origin_data = data.get("response", {}).get("flightroute", {}).get("origin")
        if origin_data:
            origin_country = origin_data.get("country_name")
            if origin_country:
                return origin_country
        # fallback to airline country
        airline_data = data.get("response", {}).get("flightroute", {}).get("airline", {})
        airline_country = airline_data.get("country")
        if airline_country:
            return airline_country
    except Exception as e:
        print(f"Warning: ADSBDB callsign lookup failed for '{callsign_str}': {e}")

    return None  # not found or error

def load_passenger_capacity_mapping(pax_csv="aircraft_pax.csv"):
    """
    Loads passenger capacity from a CSV with columns: model, typecode, passenger_capacity.
    Returns a dict: pax_mapping[(model.lower(), typecode.lower())] = int capacity
    """
    mapping = {}
    if not os.path.exists(pax_csv):
        print(f"Warning: Passenger capacity file '{pax_csv}' does not exist.")
        return mapping

    try:
        with open(pax_csv, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                model = row.get('model', '').strip().lower()
                typecode = row.get('typecode', '').strip().lower()
                capacity_str = row.get('passenger_capacity', '0').strip()
                if typecode:  # allow empty model, but require typecode
                    try:
                        capacity = int(capacity_str)
                    except ValueError:
                        capacity = 0
                    mapping[(model, typecode)] = capacity
        print(f"Loaded passenger capacity for {len(mapping)} (model, typecode) entries.")
    except Exception as e:
        print(f"Error loading passenger capacity CSV '{pax_csv}': {e}")

    return mapping

def save_to_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, ensure_ascii=False, indent=4)
        print(f"\nData has been saved to '{filename}'.")
    except Exception as e:
        print(f"Error saving data to JSON file: {e}")

def save_unique_models_typecodes(flight_data, filename):
    import csv

    unique_pairs = set()
    for flight in flight_data:
        model_raw = flight.get('model', '').strip()
        typecode_raw = flight.get('typecode', '').strip()

        # Convert 'Unknown'/'N/A' => ""
        model = "" if model_raw.lower() in ["unknown", "n/a"] else model_raw
        typecode = "" if typecode_raw.lower() in ["unknown", "n/a"] else typecode_raw

        # skip if both empty
        if not model and not typecode:
            continue
        unique_pairs.add((model, typecode))

    if not unique_pairs:
        print(f"No unique (model, typecode) pairs to save in '{filename}'.")
        return

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['model', 'typecode'])
            for model, tcode in sorted(unique_pairs):
                writer.writerow([model, tcode])
        print(f"\nUnique (model, typecode) pairs have been saved to '{filename}'.")
    except Exception as e:
        print(f"Error saving unique models and typecodes to CSV: {e}")

def save_flights_by_day_text(flights_by_day, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for day_str in sorted(flights_by_day.keys()):
                f.write(f"{day_str}\n")
                for hour_str in sorted(flights_by_day[day_str].keys()):
                    data_dict = flights_by_day[day_str][hour_str]
                    fc = data_dict['flight_count']
                    pc = data_dict['passenger_count']
                    line = f"  {hour_str}: {fc} flight(s), {pc} passenger(s)\n"
                    f.write(line)
                f.write("\n")
        print(f"\nFlights-by-day breakdown has been saved to '{filename}'.")
    except Exception as e:
        print(f"Error saving flights-by-day text file: {e}")

def save_hourly_timeseries_csv(flights_by_day, filename):
    import csv

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['date', 'hour', 'flight_count', 'passenger_count'])

            for day_str in sorted(flights_by_day.keys()):
                date_part = day_str.split()[0]  # 'YYYY-MM-DD'
                for hour in range(24):
                    hour_str = f"{hour:02d}:00:00"
                    if hour_str in flights_by_day[day_str]:
                        data_dict = flights_by_day[day_str][hour_str]
                        fc = data_dict['flight_count']
                        pc = data_dict['passenger_count']
                    else:
                        fc = 0
                        pc = 0

                    writer.writerow([date_part, hour_str, fc, pc])

        print(f"\nHourly time-series data saved to '{filename}'.")
    except Exception as e:
        print(f"Error saving hourly time-series CSV: {e}")

@retry(
    wait=wait_exponential(multiplier=WAIT_MULTIPLIER, min=1, max=WAIT_MAX),
    stop=stop_after_attempt(MAX_RETRIES),
    retry=retry_if_exception_type((requests.exceptions.HTTPError,
                                   requests.exceptions.ConnectionError,
                                   requests.exceptions.Timeout)),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True
)
def fetch_arrivals(airport, begin_time, end_time, auth):
    url = "https://opensky-network.org/api/flights/arrival"
    params = {
        'airport': airport,
        'begin': begin_time,
        'end': end_time
    }
    # Encode the parameters
    query_string = urlencode(params)

    # Construct the full URL
    full_url = f"{url}?{query_string}"

    # Print the full URL with parameters
    print("Full URL with parameters:")
    print(full_url)
    response = requests.get(url, params=params, auth=auth, timeout=30)
    response.raise_for_status()
    return response.json()

def format_time(timestamp, tz_info):
    if not timestamp:
        return "N/A", "N/A", None
    dt_utc = datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=ZoneInfo("UTC"))
    dt_local = dt_utc.astimezone(tz_info)
    formatted_utc = dt_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
    formatted_local = dt_local.strftime('%Y-%m-%d %H:%M:%S %Z')
    return formatted_utc, formatted_local, dt_local

def get_country_for_callsign(callsign, icao24, local_map):
    #if not callsign:
    #    return None

    # Normalize inputs
    callsign_upper = callsign.strip().upper().replace(" ", "")
    icao24_lower = icao24.strip().lower()

    # Check if local lookup is enabled and if icao24 exists in local_map
    if LOCAL_CALLSIGN_COUNTRY_ENABLED:
        aircraft_info = local_map.get(icao24_lower)
        if aircraft_info and 'country' in aircraft_info:
            return aircraft_info['country']

        # Fallback to ADSBDB API
    country = get_country_from_adsbdb(callsign_upper)
    return country


def is_international_flight(flight, callsign_map):
    est_dep = flight.get('estDepartureAirport')
    if est_dep is not None:
        return not est_dep.startswith("Y")
    else:
        # departure is null => callsign approach
        csign = flight.get('callsign') or ""
        icao24 = flight.get('icao24') or ""
        country = get_country_for_callsign(csign, icao24, callsign_map)
        if country and country.lower() != "australia":
            return True
        return False

def main():
    print("=== OpenSky Network International Arrival Flights Fetcher ===\n")

    # Load icao24->(country,model,typecode)
    csv_mapping = csv_lookup.load_csv_mapping("icao_to_aircraft.csv")
    if not csv_mapping:
        print("Error: Could not load icao_to_aircraft.csv. Exiting.")
        return

    # If you have callsign->country in a local CSV, load it:
    #callsign_map = {}
    #if LOCAL_CALLSIGN_COUNTRY_ENABLED:
        #callsign_map = load_callsign_country("icao_to_aircraft.csv")

    # Load passenger capacities
    pax_mapping = load_passenger_capacity_mapping("aircraft_pax.csv")

    # Prompt for credentials
    print("Please enter your OpenSky Network credentials.")
    username = "jdcabusas"
    password = "Joejoe201!"
    if not username or not password:
        print("Error: Username/password cannot be empty.")
        return
    auth = (username, password)

    # Airport Code
    airport = input("Enter the airport ICAO code (e.g., 'YPAD'): ").strip().upper()
    if not airport:
        print("Error: Airport code cannot be empty.")
        return
    if len(airport) != 4 or not airport.startswith('Y'):
        print("Warning: Make sure this is a valid Australian ICAO code.\n")

    # Number of days
    try:
        x_days = int(input("Enter # of days in the past to fetch arrivals: ").strip())
        if x_days <= 0:
            print("Error: Number of days must be positive.")
            return
    except ValueError:
        print("Error: invalid integer for number of days.")
        return

    try:
        adelaide_tz = ZoneInfo("Australia/Adelaide")
    except Exception as e:
        print(f"Error loading Adelaide timezone: {e}")
        return

    now_utc = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    end_time_dt = now_utc
    begin_time_dt = now_utc - datetime.timedelta(days=x_days)

    all_arrivals = []
    max_days_per_call = 7
    total_calls = (x_days + max_days_per_call - 1) // max_days_per_call

    print(f"\nTotal API calls: {total_calls}\n")

    for i in range(total_calls):
        days_to_fetch = min(max_days_per_call, x_days - i * max_days_per_call)
        call_end_dt = end_time_dt - datetime.timedelta(days=i * max_days_per_call)
        call_begin_dt = call_end_dt - datetime.timedelta(days=days_to_fetch)

        call_begin_unix = int(call_begin_dt.timestamp())
        call_end_unix = int(call_end_dt.timestamp())

        print(f"API Call {i+1}: {call_begin_dt} to {call_end_dt}")

        try:
            arrivals = fetch_arrivals(airport, call_begin_unix, call_end_unix, auth)
            all_arrivals.extend(arrivals)
            print(f"  Retrieved {len(arrivals)} flights.\n")
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 401:
                print("Authentication failed. Check credentials.")
                return
            else:
                print(f"HTTP error: {http_err} - {http_err.response.text}")
        except requests.exceptions.Timeout as to_err:
            print(f"Timeout error after {MAX_RETRIES} retries: {to_err}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        if i < total_calls - 1:
            print("Wait 10 seconds...\n")
            time.sleep(10)

    if not all_arrivals:
        print("No flights data from the API.")
        return

    total_flights_fetched = len(all_arrivals)

    # Filter for "international"
    international_flights = []
    for f in all_arrivals:
        if is_international_flight(f, csv_mapping):
            international_flights.append(f)

    total_international = len(international_flights)
    if total_international == 0:
        print("No international flights found.")
        return

    print(f"Found {total_international} international flights.\n")
    flight_data_for_json = []

    flights_by_day = defaultdict(lambda: defaultdict(lambda: {'flight_count':0,'passenger_count':0}))

    for flight in international_flights:
        icao24 = flight.get('icao24', 'N/A')
        first_seen_ts = flight.get('firstSeen', 0)
        last_seen_ts = flight.get('lastSeen', 0)
        est_arrival = flight.get('estArrivalAirport','N/A')
        est_departure = flight.get('estDepartureAirport','N/A')
        callsign = flight.get('callsign','N/A').strip() if flight.get('callsign') else 'N/A'

        # Times
        first_utc, first_local, _ = format_time(first_seen_ts, adelaide_tz)
        last_utc, last_local, dt_local = format_time(last_seen_ts, adelaide_tz)

        # CSV: icao24 -> {country,model,typecode}
        icao24_clean = icao24.lower().strip()
        if icao24_clean in csv_mapping:
            country_val = csv_mapping[icao24_clean]['country']
            model_val   = csv_mapping[icao24_clean]['model']
            typecode_val= csv_mapping[icao24_clean]['typecode']
        else:
            country_val = "Unknown"
            model_val   = "Unknown"
            typecode_val= "Unknown"

        # passenger capacity two-step approach
        model_lc = model_val.lower().strip()
        typecode_lc = typecode_val.lower().strip()
        capacity = None
        if (model_lc, typecode_lc) in pax_mapping:
            capacity = pax_mapping[(model_lc, typecode_lc)]
        if capacity is None:
            capacity = pax_mapping.get(("", typecode_lc), 0)

        # Aggregate
        if dt_local:
            day_str  = dt_local.strftime('%Y-%m-%d %A')
            hour_str = dt_local.strftime('%H:00:00')
            flights_by_day[day_str][hour_str]['flight_count']    += 1
            flights_by_day[day_str][hour_str]['passenger_count'] += capacity

        print(f"Flight: {callsign}, ICAO24: {icao24}")
        print(f"  Origin: {est_departure}")
        print(f"  Destination: {est_arrival}")
        print(f"  First Seen: {first_utc} | {first_local}")
        print(f"  Last Seen:  {last_utc} | {last_local}")
        print(f"  Country:    {country_val}")
        print(f"  Model:      {model_val}")
        print(f"  Typecode:   {typecode_val}")
        print(f"  Passenger Capacity: {capacity}")
        print("-"*50)

        flight_data_for_json.append({
            "callsign": callsign,
            "icao24": icao24,
            "origin": est_departure,
            "destination": est_arrival,
            "first_seen_utc": first_utc,
            "first_seen_local": first_local,
            "last_seen_utc": last_utc,
            "last_seen_local": last_local,
            "country": country_val,
            "model": model_val,
            "typecode": typecode_val,
            "passenger_capacity": capacity
        })

    print("\n== Summary ==")
    print(f"Total Flights Fetched: {total_flights_fetched}")
    print(f"International Flights: {total_international}\n")

    # Summaries
    for day_str in sorted(flights_by_day.keys()):
        print(day_str)
        for hour_str in sorted(flights_by_day[day_str].keys()):
            d = flights_by_day[day_str][hour_str]
            print(f"  {hour_str}: {d['flight_count']} flights, {d['passenger_count']} passengers")
        print()

    # Save outputs
    ts_str = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    flights_filename = os.path.join(data_dir,f"international_flights_{ts_str}.json")
    save_to_json(flight_data_for_json, flights_filename)

    txt_filename = os.path.join(data_dir,f"international_flights_by_day_{ts_str}.txt")
    save_flights_by_day_text(flights_by_day, txt_filename)

    if SAVE_HOURLY_TIMESERIES:
        csv_filename = os.path.join(data_dir,f"hourly_passengers_{ts_str}.csv")
        save_hourly_timeseries_csv(flights_by_day, csv_filename)

    if SAVE_UNIQUE_MODELS_TYPECODES:
        unique_csvfile = os.path.join(data_dir,f"unique_models_typecodes_{ts_str}.csv")
        save_unique_models_typecodes(flight_data_for_json, unique_csvfile)


if __name__ == "__main__":
    main()
