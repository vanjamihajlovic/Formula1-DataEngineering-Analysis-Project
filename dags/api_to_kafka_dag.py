import json
import time
import requests
from airflow.decorators import dag
from pendulum import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

API_URLS = {
    "status": "https://ergast.com/api/f1/status.json",
    "driver": "http://api.jolpi.ca/ergast/f1/drivers.json",
    "race": "http://api.jolpi.ca/ergast/f1/races.json",
    "constructor": "https://ergast.com/api/f1/constructors.json",
    "laptimes": "https://ergast.com/api/f1/2024",
    "driverstandings": "https://ergast.com/api/f1/2024/driverStandings.json",
    "constandings": "https://ergast.com/api/f1/2024/constructorStandings.json",
    "pitstops": "https://ergast.com/api/f1/2024",
    "results": "https://ergast.com/api/f1/2024"  # Base URL for results
}

KAFKA_TOPICS = {
    "status": "status_topic",
    "driver": "driver_topic",
    "race": "race_topic",
    "constructor": "constructor_topic",
    "laptimes": "laptimes_topic",
    "driverstandings": "driverstandings_topic",
    "constandings": "constandings_topic",
    "pitstops": "pitstops_topic",
    "results": "results_topic"  
}

KEY_FIELDS = {
    "status": "statusId",
    "driver": "driverId",
    "race": "raceId",
    "constructor": "constructorId",
    "laptimes": None,
    "driverstandings": "driverId",
    "constandings": "constructorId",
    "pitstops": None,
    "results": None  # We’ll use driverId_raceId as key
}

PAYLOAD_PATHS = {
    "status": ["MRData", "StatusTable", "Status"],
    "driver": ["MRData", "DriverTable", "Drivers"],
    "race": ["MRData", "RaceTable", "Races"],
    "constructor": ["MRData", "ConstructorTable", "Constructors"],
    "laptimes": ["MRData", "RaceTable", "Races"],
    "driverstandings": ["MRData", "StandingsTable", "StandingsLists"],
    "constandings": ["MRData", "StandingsTable", "StandingsLists"],
    "pitstops": ["MRData", "RaceTable", "Races"],
    "results": ["MRData", "RaceTable", "Races"]
}

def extract_and_produce(api_url, data_type, topic, **kwargs):
    producer = KafkaProducer(
        bootstrap_servers=['kafka1:39092', 'kafka2:39093', 'kafka3:39094'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        acks='all',
        retries=3
    )

    payload_path = PAYLOAD_PATHS.get(data_type)
    key_field = KEY_FIELDS.get(data_type, "unknown")
    if not payload_path:
        raise ValueError(f"⚠️ Unknown data type: {data_type}")

    print(f"🔹 [EXTRACT] Starting extraction | API: {api_url} | Data Type: {data_type}")

    if data_type == "laptimes":
        total_processed = 0
        for round_num in range(1, 25):
            lap_num = 1
            while True:
                url = f"{api_url}/{round_num}/laps/{lap_num}.json"
                for attempt in range(3):
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        print(f"⚠️ [RATE LIMIT] 429 on attempt {attempt + 1} | URL: {url}. Retrying in {2 ** attempt}s...")
                        time.sleep(2 ** attempt)
                    elif response.status_code == 404:
                        print(f"ℹ️ [END] No more laps for round {round_num} at lap {lap_num}")
                        break
                    else:
                        raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {url}")
                else:
                    raise ValueError(f"❌ [API ERROR] Exhausted retries | URL: {url}")

                if response.status_code == 404:
                    break

                data = response.json()
                races = data["MRData"]["RaceTable"]["Races"]
                if not races or not races[0].get("Laps"):
                    print(f"ℹ️ [END] No lap data for round {round_num}, lap {lap_num}")
                    break

                lap_data = races[0]["Laps"][0]
                for timing in lap_data["Timings"]:
                    key = f"{timing['driverId']}_{round_num}_{lap_data['number']}"
                    value = {
                        "round": round_num,
                        "lap": int(lap_data["number"]),
                        "driverId": timing["driverId"],
                        "position": int(timing["position"]),
                        "time": timing["time"]
                    }
                    producer.send(topic, key=key, value=value)
                    print(f"🔸 [PRODUCE] Sent laptime: {key}")
                    total_processed += 1
                lap_num += 1

    elif data_type == "race":
        offset = 0
        limit = 30
        total_processed = 0
        while True:
            paged_url = f"{api_url}?offset={offset}&limit={limit}"
            response = requests.get(paged_url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {paged_url}")
            data = response.json()
            races = data["MRData"]["RaceTable"]["Races"]
            if not races:
                break
            total = int(data["MRData"].get("total", 0))
            for race in races:
                if int(race["season"]) <= 2023:
                    key = f"{race['season']}_{race['round']}"
                    value = race  # Define value here
                    producer.send(topic, key=key, value=value)
                    total_processed += 1
            offset += limit
            if offset >= total:
                break
        ergast_url = "https://ergast.com/api/f1/2024.json"
        response = requests.get(ergast_url, timeout=10)
        if response.status_code != 200:
            raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {ergast_url}")
        data = response.json()
        races = data["MRData"]["RaceTable"]["Races"]
        for race in races:
            key = f"{race['season']}_{race['round']}"
            value = race  # Define value here
            producer.send(topic, key=key, value=value)
            total_processed += 1

    elif data_type == "driverstandings":
        total_processed = 0
        response = requests.get(api_url, timeout=10)
        if response.status_code != 200:
            raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {api_url}")
        
        data = response.json()
        standings_list = data["MRData"]["StandingsTable"]["StandingsLists"]
        if not standings_list:
            print(f"ℹ️ [END] No standings data for {api_url}")
            return

        season = standings_list[0]["season"]
        round_num = standings_list[0]["round"]
        driver_standings = standings_list[0]["DriverStandings"]

        for standing in driver_standings:
            driver_id = standing["Driver"]["driverId"]
            key = f"{driver_id}_{season}_{round_num}"
            value = {
                "season": int(season),
                "round": int(round_num),
                "position": standing["position"],
                "positionText": standing["positionText"],
                "points": standing["points"],
                "wins": standing["wins"],
                "Driver": standing["Driver"],
                "Constructors": standing["Constructors"]
            }
            producer.send(topic, key=key, value=value)
            print(f"🔸 [PRODUCE] Sent driver standing: {key}")
            total_processed += 1

    elif data_type == "constandings":
        total_processed = 0
        response = requests.get(api_url, timeout=10)
        if response.status_code != 200:
            raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {api_url}")
        
        data = response.json()
        standings_list = data["MRData"]["StandingsTable"]["StandingsLists"]
        if not standings_list:
            print(f"ℹ️ [END] No constructor standings data for {api_url}")
            return

        season = standings_list[0]["season"]
        round_num = standings_list[0]["round"]
        constructor_standings = standings_list[0]["ConstructorStandings"]

        for standing in constructor_standings:
            constructor_id = standing["Constructor"]["constructorId"]
            key = f"{constructor_id}_{season}_{round_num}"
            value = {
                "season": int(season),
                "round": int(round_num),
                "position": standing["position"],
                "positionText": standing["positionText"],
                "points": standing["points"],
                "wins": standing["wins"],
                "Constructor": standing["Constructor"]
            }
            producer.send(topic, key=key, value=value)
            print(f"🔸 [PRODUCE] Sent constructor standing: {key}")
            total_processed += 1

    elif data_type == "pitstops":
        total_processed = 0
        for round_num in range(1, 25):
            offset = 0
            limit = 30
            while True:
                url = f"{api_url}/{round_num}/pitstops.json?offset={offset}&limit={limit}"
                for attempt in range(3):
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        print(f"⚠️ [RATE LIMIT] 429 on attempt {attempt + 1} | URL: {url}. Retrying in {2 ** attempt}s...")
                        time.sleep(2 ** attempt)
                    elif response.status_code == 404:
                        print(f"ℹ️ [END] No pit stops for round {round_num}")
                        break
                    else:
                        raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {url}")
                else:
                    raise ValueError(f"❌ [API ERROR] Exhausted retries | URL: {url}")

                if response.status_code == 404:
                    break

                data = response.json()
                races = data["MRData"]["RaceTable"]["Races"]
                if not races or not races[0].get("PitStops"):
                    print(f"ℹ️ [END] No pit stop data for round {round_num}")
                    break

                season = races[0]["season"]
                round_num = races[0]["round"]
                pit_stops = races[0]["PitStops"]

                for pit_stop in pit_stops:
                    stop = int(pit_stop["stop"])
                    if stop > 10:  # Arbitrary max
                        print(f"⚠️ [INVALID] Skipping stop={stop} for round {round_num}, driver {pit_stop['driverId']}")
                        continue
                    key = f"{pit_stop['stop']}_{round_num}_{pit_stop['driverId']}"
                    value = {
                        "season": int(season),
                        "round": int(round_num),
                        "stop": int(pit_stop["stop"]),
                        "driverId": pit_stop["driverId"],
                        "lap": int(pit_stop["lap"]),
                        "time": pit_stop["time"],
                        "duration": pit_stop["duration"]
                    }
                    producer.send(topic, key=key, value=value)
                    print(f"🔸 [PRODUCE] Sent pit stop: {key}")
                    total_processed += 1

                offset += limit
                total = int(data["MRData"].get("total", 0))
                if offset >= total:
                    break

    elif data_type == "results":
        total_processed = 0
        for round_num in range(1, 25):  # All 24 rounds of 2024
            url = f"{api_url}/{round_num}/results.json"
            for attempt in range(3):
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    break
                elif response.status_code == 429:
                    print(f"⚠️ [RATE LIMIT] 429 on attempt {attempt + 1} | URL: {url}. Retrying in {2 ** attempt}s...")
                    time.sleep(2 ** attempt)
                elif response.status_code == 404:
                    print(f"ℹ️ [END] No results for round {round_num}")
                    break
                else:
                    raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {url}")
            else:
                raise ValueError(f"❌ [API ERROR] Exhausted retries | URL: {url}")

            if response.status_code == 404:
                break

            data = response.json()
            races = data["MRData"]["RaceTable"]["Races"]
            if not races or not races[0].get("Results"):
                print(f"ℹ️ [END] No results data for round {round_num}")
                break

            season = races[0]["season"]
            round_num = races[0]["round"]
            results = races[0]["Results"]

            for result in results:
                driver_id = result["Driver"]["driverId"]
                key = f"{driver_id}_{round_num}"
                value = {
                    "season": int(season),
                    "round": int(round_num),
                    "number": result["number"],
                    "position": result["position"],
                    "positionText": result["positionText"],
                    "points": result["points"],
                    "driverId": driver_id,
                    "constructorId": result["Constructor"]["constructorId"],
                    "grid": result["grid"],
                    "laps": result["laps"],
                    "status": result["status"],
                    "time": result.get("Time", {}).get("time") if "Time" in result else None,
                    "milliseconds": result.get("Time", {}).get("millis") if "Time" in result else None,
                    "fastestLap": {
                        "rank": result.get("FastestLap", {}).get("rank"),
                        "lap": result.get("FastestLap", {}).get("lap"),
                        "time": result.get("FastestLap", {}).get("Time", {}).get("time")
                    }
                }
                producer.send(topic, key=key, value=value)
                print(f"🔸 [PRODUCE] Sent result: {key}")
                total_processed += 1

    else:
        offset = 0
        limit = 30
        total_processed = 0
        while True:
            paged_url = f"{api_url}?offset={offset}&limit={limit}"
            response = requests.get(paged_url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"❌ [API ERROR] Code: {response.status_code} | URL: {paged_url}")
            data = response.json()
            nested_data = data
            for key in payload_path:
                nested_data = nested_data.get(key, {})
            data_list = nested_data if isinstance(nested_data, list) else []
            if not data_list:
                break
            total = int(data["MRData"].get("total", 0))
            for record in data_list:
                key = record.get(key_field, "unknown")
                value = record  # Fix: Assign the record as the value
                producer.send(topic, key=key, value=value)
                total_processed += 1
            offset += limit
            if offset >= total:
                break

    producer.flush()
    producer.close()
    print(f"✅ [EXTRACT] Finished | Total records processed: {total_processed} | Data Type: {data_type}")

@dag(
    dag_id="api_to_kafka_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)
def api_to_kafka_dag():
    for name, url in API_URLS.items():
        task = PythonOperator(
            task_id=f"extract_and_produce_{name}",
            python_callable=extract_and_produce,
            op_kwargs={"api_url": url, "data_type": name, "topic": KAFKA_TOPICS[name]},
        )

api_to_kafka_dag()