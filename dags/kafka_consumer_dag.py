import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json

# Define Kafka topics
KAFKA_TOPICS = [
    "status_topic",
    "driver_topic",
    "race_topic",
    "constructor_topic",
    "laptimes_topic",
    "driverstandings_topic",
    "constandings_topic",
    "pitstops_topic",
    "results_topic"  # New topic
]

# Configure logging
logging.basicConfig(level=logging.INFO)

@task
def consume_from_topic(topic):
    """Consume all available messages from a Kafka topic and return them."""
    logging.info(f"Starting consumer for topic: {topic}")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka1:39092', 'kafka2:39093', 'kafka3:39094'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f"{topic}_processor_group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    messages = []
    message_count = 0
    
    try:
        while True:
            messages_batch = consumer.poll(timeout_ms=1000)
            if messages_batch:
                for _, message_list in messages_batch.items():
                    for message in message_list:
                        messages.append({"key": message.key, "value": message.value})
                        message_count += 1
                        logging.info(f"Consumed message {message_count} from {topic}: Key={message.key}")
            else:
                logging.info(f"No more messages on {topic}. Total consumed: {message_count}")
                break
    except Exception as e:
        logging.error(f"Error consuming from {topic}: {str(e)}")
        raise
    finally:
        consumer.close()
        logging.info(f"Consumer closed for topic: {topic}. Total messages read: {message_count}")
    
    return messages if messages else []


@task
def process_results_data(messages):
    """Transform and load results data into FormulaOne and update race table."""
    if not messages:
        logging.info("No results messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    # Maps for foreign keys
    driver_map = {row[0]: row[1] for row in hook.get_records("SELECT driverRef, driverId FROM driver")}
    constructor_map = {row[0]: row[1] for row in hook.get_records("SELECT constructorRef, constructorId FROM constructor")}
    status_map = {row[0]: row[1] for row in hook.get_records("SELECT status_name, statusId FROM status")}
    logging.info(f"Status map contents: {status_map}")  # This should log the map

    race_map = {(row[0], row[1]): row[2] for row in hook.get_records("SELECT year, round, race_id FROM race WHERE year = 2024")}
    pitstop_map = {(row[0], row[1], row[2]): row[3] for row in hook.get_records("SELECT driverId, race_id, stop, lap_pitstops FROM pitStops")}
    laptime_map = {(row[0], row[1], row[2]): row[0] for row in hook.get_records("SELECT driverId, race_id, lap FROM lapTimes")}

    # Get max resultId
    max_id_query = "SELECT MAX(resultId) FROM FormulaOne;"
    max_id = hook.get_first(max_id_query)[0] or 0
    next_id = max_id + 1

    transformed = []
    race_updates = {}
    for msg in messages:
        value = msg["value"]
        driver_id = driver_map.get(value["driverId"])
        if not driver_id:
            logging.warning(f"Driver {value['driverId']} not found, skipping")
            continue

        race_id = race_map.get((value["season"], value["round"]))
        if not race_id:
            logging.warning(f"Race {value['season']}/{value['round']} not found, skipping")
            continue

        constructor_id = constructor_map.get(value["constructorId"])
        status_id = status_map.get(value["status"])
        if status_id is None:  # Error occurs here if status_id isn’t set
            logging.warning(f"Status '{value['status']}' not found in status_map")
        position = value["position"]
        time_str = value.get("time")

        
        time_value = time_str
        milliseconds = value["milliseconds"] if value["milliseconds"] and time_value else None

        # Pit stop lookup (optional, first stop if exists)
        stop = None
        for (d_id, r_id, s) in pitstop_map.keys():  # Fix: Unpack 3 values
            if d_id == driver_id and r_id == race_id:
                stop = s
                break

        # Lap time lookup (use total laps for this result)
        lap = int(value["laps"]) if laptime_map.get((driver_id, race_id, int(value["laps"]))) else None

        transformed.append({
            "resultId": next_id,
            "raceId": race_id,
            "driverId": driver_id,
            "constructorId": constructor_id,
            "statusId": status_id,
            "driverStandingsId": None,
            "conStandingsId": None,
            "grid": value["grid"],
            "stop": stop,
            "position": position,
            "rank": value["fastestLap"]["rank"] if value["fastestLap"]["rank"] else None,
            "time": time_value,
            "milliseconds": milliseconds,
            "points": int(value["points"]),
            "number": int(value["number"]),
            "lap": lap,
            "positionOrder": int(position) if position.isdigit() else None,
            "positionText": int(value["positionText"]) if value["positionText"].isdigit() else None,
            "wins": 1 if position == "1" else 0
        })

        # Collect race updates (laps, fastest lap)
        race_key = (value["season"], value["round"])
        if race_key not in race_updates:
            race_updates[race_key] = {
                "race_id": race_id,
                "laps": int(value["laps"]),
                "fastestLap": None,
                "fastestLapTime": None,
                "fastestLapSpeed" : None #new field for average speed
            }
        rank = value.get("FastestLap", {}).get("rank")
        logging.info(f"Checking rank for {value['season']}/{value['round']}: {rank}")
        if str(rank) == "1":  # Convert to string to match "1" or 1
            fastest_lap_data = value["FastestLap"]
            logging.info(f"Rank 1 data for {value['season']}/{value['round']}: {fastest_lap_data}")
            if "lap" in fastest_lap_data:
                race_updates[race_key]["fastestLap"] = int(fastest_lap_data["lap"])
            if "Time" in fastest_lap_data and "time" in fastest_lap_data["Time"]:
                race_updates[race_key]["fastestLapTime"] = fastest_lap_data["Time"]["time"]
            if "AverageSpeed" in fastest_lap_data and "speed" in fastest_lap_data["AverageSpeed"]:
                race_updates[race_key]["fastestLapSpeed"] = float(fastest_lap_data["AverageSpeed"]["speed"])
            else:
                logging.warning(f"No AverageSpeed for rank 1 in {value['season']}/{value['round']}: {fastest_lap_data}")
            logging.info(f"Set race_updates[{race_key}]: {race_updates[race_key]}")

        next_id += 1

    # Insert into FormulaOne
    if transformed:
        rows_to_insert = [
            (r["resultId"], r["raceId"], r["driverId"], r["constructorId"], r["statusId"],
             r["driverStandingsId"], r["conStandingsId"], r["grid"], r["stop"], r["position"],
             r["rank"], r["time"], r["milliseconds"], r["points"], r["number"], r["lap"],
             r["positionOrder"], r["positionText"], r["wins"])
            for r in transformed
        ]
        hook.insert_rows(
            table="FormulaOne",
            rows=rows_to_insert,
            target_fields=["resultId", "raceId", "driverId", "constructorId", "statusId",
                           "driverStandingsId", "conStandingsId", "grid", "stop", "position",
                           "rank", "time", "milliseconds", "points", "number", "lap",
                           "positionOrder", "positionText", "wins"]
        )
        logging.info(f"Inserted {len(rows_to_insert)} new result records")

    # Update race table
    for (season, round_num), update in race_updates.items():
        # Only run update if all rank 1 data is present
        if all(update[key] is not None for key in ["laps", "fastestLap", "fastestLapTime", "fastestLapSpeed"]):
            hook.run(
                """
                UPDATE race
                SET laps = %s, 
                    fastestLap = %s, 
                    fastestLapTime = %s, 
                    fastestLapSpeed = %s
                WHERE race_id = %s 
                AND (laps IS NULL OR fastestLap IS NULL OR fastestLapTime IS NULL OR fastestLapSpeed IS NULL);
                """,
                parameters=(update["laps"], update["fastestLap"], update["fastestLapTime"], update["fastestLapSpeed"], update["race_id"])
            )
            logging.info(f"Updated race {season}/{round_num}: laps={update['laps']}, fastestLap={update['fastestLap']}, fastestLapTime={update['fastestLapTime']}, fastestLapSpeed={update['fastestLapSpeed']}")
        else:
            logging.warning(f"Skipping race update for {season}/{round_num} due to incomplete rank 1 data: {update}")

    return "Results data processed and loaded."


@task
def process_pitstops_data(messages):
    """Transform and load pit stop data from Kafka into PostgreSQL."""
    if not messages:
        logging.info("No pit stop messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    # Map driverRef to driverId and race year/round to race_id
    driver_map = {row[0]: row[1] for row in hook.get_records("SELECT driverRef, driverId FROM driver")}
    race_map = {(row[0], row[1]): row[2] for row in hook.get_records("SELECT year, round, race_id FROM race WHERE year = 2024")}

    # Check existing records to avoid duplicates
    existing_keys_query = "SELECT stop, race_id, driverId FROM pitStops;"
    existing_keys = {(row[0], row[1], row[2]) for row in hook.get_records(existing_keys_query)}

    transformed = []
    for msg in messages:
        value = msg["value"]
        driver_id = driver_map.get(value["driverId"])
        if not driver_id:
            logging.warning(f"Driver {value['driverId']} not found, skipping")
            continue

        race_id = race_map.get((value["season"], value["round"]))
        if not race_id:
            logging.warning(f"Race {value['season']}/{value['round']} not found, skipping")
            continue

        stop = value["stop"]
        key = (stop, race_id, driver_id)
        if key in existing_keys:
            logging.info(f"Skipping duplicate pit stop: stop={stop}, race_id={race_id}, driverId={driver_id}")
            continue

        # Handle duration conversion (MM:SS.sss or SS.sss)
        duration_str = value["duration"]
        if ':' in duration_str:  # Format like "1:14.773"
            minutes, seconds = duration_str.split(':')
            duration_seconds = float(minutes) * 60 + float(seconds)
        else:  # Format like "23.004"
            duration_seconds = float(duration_str)

        milliseconds = int(duration_seconds * 1000)

        transformed.append({
            "stop": stop,
            "race_id": race_id,
            "driverId": driver_id,
            "lap_pitstops": value["lap"],
            "time_pitstops": value["time"],
            "duration": int(duration_seconds),  # Integer seconds
            "milliseconds_pitstops": milliseconds
        })

    if transformed:
        rows_to_insert = [
            (r["stop"], r["race_id"], r["driverId"], r["lap_pitstops"], r["time_pitstops"],
             r["duration"], r["milliseconds_pitstops"])
            for r in transformed
        ]
        hook.insert_rows(
            table="pitStops",
            rows=rows_to_insert,
            target_fields=["stop", "race_id", "driverId", "lap_pitstops", "time_pitstops",
                           "duration", "milliseconds_pitstops"]
        )
        logging.info(f"Inserted {len(rows_to_insert)} new pit stop records")
        for row in rows_to_insert:
            print(f"Inserted: stop={row[0]}, race_id={row[1]}, driverId={row[2]}, lap={row[3]}, time={row[4]}")
    else:
        logging.info("No new pit stops to insert")

    return "Pit stop data processed and loaded."

@task
def process_constandings_data(messages):
    """Transform and load constructor standings from Kafka into PostgreSQL and update FormulaOne."""
    if not messages:
        logging.info("No constructor standings messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    # Maps for foreign keys
    constructor_map = {row[0]: row[1] for row in hook.get_records("SELECT constructorRef, constructorId FROM constructor")}
    race_map = {(row[0], row[1]): row[2] for row in hook.get_records("SELECT year, round, race_id FROM race WHERE year = 2024")}

    # Get existing conStandingsId to avoid duplicates
    existing_ids_query = "SELECT conStandingsId FROM conStandings;"
    existing_ids = {row[0] for row in hook.get_records(existing_ids_query)}
    max_id_query = "SELECT MAX(conStandingsId) FROM conStandings;"
    max_id = hook.get_first(max_id_query)[0] or 0
    next_id = max_id + 1

    transformed = []
    formula_one_updates = []  # Store updates for FormulaOne
    seen_positions = set()

    for msg in messages:
        value = msg["value"]
        logging.info(f"Processing constructor standings: {value}")

        # Extract season and round from StandingsTable if present, otherwise from top-level
        season = value.get("season") or value.get("StandingsTable", {}).get("season")
        round_num = value.get("round") or value.get("StandingsTable", {}).get("round")

        # If it's the full API response, dig into ConstructorStandings
        if "StandingsTable" in value and "StandingsLists" in value["StandingsTable"]:
            standings_list = value["StandingsTable"]["StandingsLists"][0]  # Assuming one list per message
            season = standings_list["season"]
            round_num = standings_list["round"]
            constructor_standings = standings_list["ConstructorStandings"]
        else:
            constructor_standings = [value]  # Single record case

        for standing in constructor_standings:
            position = int(standing["position"])
            if position in seen_positions:
                logging.info(f"Skipping duplicate position {position} in this batch")
                continue
            seen_positions.add(position)

            constructor_ref = standing["Constructor"]["constructorId"]  # API uses constructorId as ref
            constructor_id = constructor_map.get(constructor_ref)
            if not constructor_id:
                logging.warning(f"Constructor {constructor_ref} not found, skipping")
                continue

            race_id = race_map.get((season, round_num))
            if not race_id:
                logging.warning(f"Race {season}/{round_num} not found, skipping")
                continue

            # Generate unique conStandingsId
            while next_id in existing_ids:
                next_id += 1
            existing_ids.add(next_id)

            # Build conStandings record
            transformed.append({
                "conStandingsId": next_id,
                "points_constructorstandings": int(standing["points"]),
                "position_constructorstandings": position,
                "positionText_constructorstandings": int(standing["positionText"]),
                "wins_constructorstandings": int(standing["wins"])
            })

            # Queue update for FormulaOne
            formula_one_updates.append({
                "conStandingsId": next_id,
                "raceId": race_id,
                "constructorId": constructor_id,
                "season": season,
                "round": round_num
            })

            next_id += 1

    # Insert into conStandings
    if transformed:
        rows_to_insert = [
            (r["conStandingsId"], r["points_constructorstandings"], r["position_constructorstandings"],
             r["positionText_constructorstandings"], r["wins_constructorstandings"])
            for r in transformed
        ]
        hook.insert_rows(
            table="conStandings",
            rows=rows_to_insert,
            target_fields=["conStandingsId", "points_constructorstandings", "position_constructorstandings",
                           "positionText_constructorstandings", "wins_constructorstandings"]
        )
        logging.info(f"Inserted {len(rows_to_insert)} new constructor standings records")
        for row in rows_to_insert:
            print(f"Inserted: conStandingsId={row[0]}, points={row[1]}, position={row[2]}, positionText={row[3]}, wins={row[4]}")

    # Update FormulaOne with conStandingsId using season, round, and constructorId
    if formula_one_updates:
        for update in formula_one_updates:
            update_query = """
                UPDATE FormulaOne f
                SET conStandingsId = %s
                FROM race r
                WHERE f.raceId = r.race_id
                AND r.year = %s
                AND r.round = %s
                AND f.constructorId = %s
                AND (f.conStandingsId IS NULL OR f.conStandingsId != %s);
            """
            hook.run(
                update_query,
                parameters=(update["conStandingsId"], update["season"], update["round"],
                           update["constructorId"], update["conStandingsId"])
            )
            logging.info(f"Updated FormulaOne: conStandingsId={update['conStandingsId']}, "
                        f"season={update['season']}, round={update['round']}, "
                        f"constructorId={update['constructorId']}")
        logging.info(f"Updated {len(formula_one_updates)} rows in FormulaOne with conStandingsId")

    return "Constructor standings data processed and loaded, FormulaOne updated."

@task
def process_driverstandings_data(messages):
    """Transform and load driver standings from Kafka into PostgreSQL and update FormulaOne."""
    if not messages:
        logging.info("No driver standings messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    # Maps for foreign keys
    driver_map = {row[0]: row[1] for row in hook.get_records("SELECT driverRef, driverId FROM driver")}
    race_map = {(row[0], row[1]): row[2] for row in hook.get_records("SELECT year, round, race_id FROM race WHERE year = 2024")}

    # Get existing driverStandingsId to avoid duplicates
    existing_ids_query = "SELECT driverStandingsId FROM driverStandings;"
    existing_ids = {row[0] for row in hook.get_records(existing_ids_query)}
    max_id_query = "SELECT MAX(driverStandingsId) FROM driverStandings;"
    max_id = hook.get_first(max_id_query)[0] or 0
    next_id = max_id + 1

    transformed = []
    formula_one_updates = []  # Store updates for FormulaOne
    seen_positions = set()  # Track positions to avoid in-batch duplicates

    for msg in messages:
        value = msg["value"]
        logging.info(f"Processing driver standings: {value}")

        # Extract season and round from StandingsTable if present, otherwise from DriverStandings
        season = value.get("season") or value.get("StandingsTable", {}).get("season")
        round_num = value.get("round") or value.get("StandingsTable", {}).get("round")
        
        # If it's the full API response, dig into DriverStandings
        if "StandingsTable" in value and "StandingsLists" in value["StandingsTable"]:
            standings_list = value["StandingsTable"]["StandingsLists"][0]  # Assuming one list
            season = standings_list["season"]
            round_num = standings_list["round"]
            driver_standings = standings_list["DriverStandings"]
        else:
            driver_standings = [value]  # Single record case

        for standing in driver_standings:
            position = int(standing["position"])
            if position in seen_positions:
                logging.info(f"Skipping duplicate position {position} in this batch")
                continue
            seen_positions.add(position)

            driver_ref = standing["Driver"]["driverId"]  # Assuming driverId is driverRef here
            driver_id = driver_map.get(driver_ref)
            if not driver_id:
                logging.warning(f"Driver {driver_ref} not found, skipping")
                continue

            race_id = race_map.get((season, round_num))
            if not race_id:
                logging.warning(f"Race {season}/{round_num} not found, skipping")
                continue

            # Generate unique driverStandingsId
            while next_id in existing_ids:
                next_id += 1
            existing_ids.add(next_id)

            # Build driverStandings record
            transformed.append({
                "driverStandingsId": next_id,
                "points_driverstandings": int(standing["points"]),
                "position_driverstandings": position,
                "positionText_driverstandings": int(standing["positionText"])
            })

            # Queue update for FormulaOne
            formula_one_updates.append({
                "driverStandingsId": next_id,
                "raceId": race_id,
                "driverId": driver_id,
                "season": season,
                "round": round_num
            })

            next_id += 1

    # Insert into driverStandings
    if transformed:
        rows_to_insert = [
            (r["driverStandingsId"], r["points_driverstandings"], r["position_driverstandings"], 
             r["positionText_driverstandings"])
            for r in transformed
        ]
        hook.insert_rows(
            table="driverStandings",
            rows=rows_to_insert,
            target_fields=["driverStandingsId", "points_driverstandings", "position_driverstandings", 
                           "positionText_driverstandings"]
        )
        logging.info(f"Inserted {len(rows_to_insert)} new driver standings records")
        for row in rows_to_insert:
            print(f"Inserted: driverStandingsId={row[0]}, points={row[1]}, position={row[2]}")

    # Update FormulaOne with driverStandingsId using season, round, and driverId
    if formula_one_updates:
        for update in formula_one_updates:
            update_query = """
                UPDATE FormulaOne f
                SET driverStandingsId = %s
                FROM race r
                WHERE f.raceId = r.race_id
                AND r.year = %s
                AND r.round = %s
                AND f.driverId = %s
                AND (f.driverStandingsId IS NULL OR f.driverStandingsId != %s);
            """
            hook.run(
                update_query,
                parameters=(update["driverStandingsId"], update["season"], update["round"], 
                           update["driverId"], update["driverStandingsId"])
            )
            logging.info(f"Updated FormulaOne: driverStandingsId={update['driverStandingsId']}, "
                        f"season={update['season']}, round={update['round']}, "
                        f"driverId={update['driverId']}")
        logging.info(f"Updated {len(formula_one_updates)} rows in FormulaOne with driverStandingsId")

    return "Driver standings data processed and loaded, FormulaOne updated."

@task
def process_laptimes_data(messages):
    """Transform and load lap times from Kafka into PostgreSQL, skipping existing entries."""
    if not messages:
        logging.info("No lap time messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    driver_query = "SELECT driverRef, driverId FROM driver;"
    driver_map = {row[0]: row[1] for row in hook.get_records(driver_query)}
    race_query = "SELECT year, round, race_id FROM race WHERE year = 2024;"
    race_map = {(row[0], row[1]): row[2] for row in hook.get_records(race_query)}

    existing_laptimes_query = "SELECT driverId, race_id, lap FROM lapTimes;"
    existing_keys = {(row[0], row[1], row[2]) for row in hook.get_records(existing_laptimes_query)}

    laptimes_transformed = []
    seen_keys = set()

    for msg in messages:
        value = msg['value']
        round_num = value["round"]
        lap = value["lap"]
        driver_ref = value["driverId"]
        driver_id = driver_map.get(driver_ref)
        if not driver_id:
            logging.warning(f"Driver {driver_ref} not found in driver table, skipping")
            continue

        race_id = race_map.get((2024, round_num))
        if not race_id:
            logging.warning(f"Race 2024/{round_num} not found in race table, skipping")
            continue

        key = (driver_id, race_id, lap)
        if key in seen_keys or key in existing_keys:
            logging.info(f"Skipping duplicate lap time: driverId={driver_id}, race_id={race_id}, lap={lap}")
            continue
        seen_keys.add(key)

        time_str = value["time"]
        minutes, seconds = time_str.split(":")
        total_seconds = int(minutes) * 60 + float(seconds)
        milliseconds = int(total_seconds * 1000)

        laptimes_transformed.append({
            "driverId": driver_id,
            "race_id": race_id,
            "lap": lap,
            "position_laptimes": value["position"],
            "time_laptimes": time_str,
            "milliseconds_laptimes": milliseconds
        })

    if laptimes_transformed:
        laptimes_rows = [(r["driverId"], r["race_id"], r["lap"], r["position_laptimes"], r["time_laptimes"], r["milliseconds_laptimes"])
                         for r in laptimes_transformed]
        hook.insert_rows(
            table="lapTimes",
            rows=laptimes_rows,
            target_fields=["driverId", "race_id", "lap", "position_laptimes", "time_laptimes", "milliseconds_laptimes"]
        )
        logging.info(f"Inserted {len(laptimes_rows)} new lap time records")
        for row in laptimes_rows:
            print(f"Inserted lap: driverId={row[0]}, race_id={row[1]}, lap={row[2]}, time={row[4]}")

    return "Lap times processed and loaded."

@task
def process_status_data(messages):
    """Transform and load new status data from Kafka into PostgreSQL."""
    if not messages:
        logging.info("No status messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")
    
    existing_names_query = "SELECT status_name FROM status;"
    existing_names = {row[0] for row in hook.get_records(existing_names_query)}
    max_id_query = "SELECT MAX(statusid) FROM status;"
    max_status_id = hook.get_first(max_id_query)[0] or 0
    next_status_id = max_status_id + 1

    transformed = []
    seen_status_names = set()
    for msg in messages:
        kafka_status_id = msg["value"].get("statusId", msg["key"])
        try:
            int(kafka_status_id)
        except (ValueError, TypeError):
            logging.warning(f"Invalid statusId '{kafka_status_id}' for message {msg}, skipping")
            continue
        
        status_name = msg["value"].get("status", "Unknown")
        if status_name in existing_names or status_name in seen_status_names:
            logging.info(f"Skipping duplicate status_name: {status_name}")
            continue
        
        seen_status_names.add(status_name)
        transformed.append({"statusId": next_status_id, "status_name": status_name})
        next_status_id += 1

    if transformed:
        rows_to_insert = [(record["statusId"], record["status_name"]) for record in transformed]
        hook.insert_rows(table="status", rows=rows_to_insert, target_fields=["statusid", "status_name"])
        print("New data inserted into status table:")
        for row in rows_to_insert:
            print(f"Inserted: statusId={row[0]}, status_name={row[1]}")
        logging.info(f"Inserted {len(rows_to_insert)} new status records")
    return "Status data processed and loaded."

@task
def process_driver_data(messages):
    """Transform and load new driver data from Kafka into PostgreSQL."""
    if not messages:
        logging.info("No driver messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")
    existing_refs_query = "SELECT driverref FROM driver;"
    existing_refs = {row[0] for row in hook.get_records(existing_refs_query)}
    max_id_query = "SELECT MAX(driverid) FROM driver;"
    max_driver_id = hook.get_first(max_id_query)[0] or 0
    next_driver_id = max_driver_id + 1

    transformed = []
    seen_driver_refs = set()
    for msg in messages:
        driver_ref = msg["value"].get("driverId", msg["key"])
        if driver_ref in existing_refs or driver_ref in seen_driver_refs:
            logging.info(f"Skipping driverref {driver_ref}")
            continue
        seen_driver_refs.add(driver_ref)
        number_drivers = msg["value"].get("permanentNumber")
        try:
            number_drivers = int(number_drivers) if number_drivers else None
        except (ValueError, TypeError):
            number_drivers = None
        dob = msg["value"].get("dateOfBirth")
        if dob:
            dob = f"{dob}T00:00:00"
        transformed.append({
            "driverid": next_driver_id,
            "driverref": driver_ref,
            "number_drivers": number_drivers,
            "code": msg["value"].get("code"),
            "forename": msg["value"].get("givenName", "Unknown"),
            "surname": msg["value"].get("familyName", "Unknown"),
            "dob": dob,
            "nationality": msg["value"].get("nationality"),
            "url": msg["value"].get("url")
        })
        next_driver_id += 1

    if transformed:
        rows_to_insert = [(record["driverid"], record["driverref"], record["number_drivers"], record["code"],
                          record["forename"], record["surname"], record["dob"], record["nationality"], record["url"])
                         for record in transformed]
        hook.insert_rows(table="driver", rows=rows_to_insert,
                         target_fields=["driverid", "driverref", "number_drivers", "code", "forename", "surname", "dob", "nationality", "url"])
        print("New data inserted into driver table:")
        for row in rows_to_insert:
            print(f"driverId={row[0]}, driverref={row[1]}, forename={row[4]}, surname={row[5]}, dob={row[6]}")
        logging.info(f"Inserted {len(rows_to_insert)} new driver records")
    return "Driver data processed and loaded."

@task
def process_race_data(messages):
    """Transform and load race data from Kafka into PostgreSQL."""
    if not messages:
        logging.info("No race messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    existing_races_query = "SELECT year, round FROM race;"
    existing_races = {(row[0], row[1]) for row in hook.get_records(existing_races_query)}
    existing_circuits_query = "SELECT circuitref, circuitid FROM circuit;"
    circuit_map = {row[0]: row[1] for row in hook.get_records(existing_circuits_query)}
    max_circuit_id = hook.get_first("SELECT MAX(circuitid) FROM circuit;")[0] or 0
    next_circuit_id = max_circuit_id + 1
    max_race_id = hook.get_first("SELECT MAX(race_id) FROM race;")[0] or 0
    next_race_id = max_race_id + 1

    race_transformed = []
    circuit_transformed = []
    fp_transformed = []
    quali_transformed = []
    sprint_transformed = []
    seen_races = set()

    for msg in messages:
        value = msg['value']
        year = int(value["season"])
        round_num = int(value["round"])
        race_key = (year, round_num)
        if race_key in existing_races or race_key in seen_races:
            logging.info(f"Skipping existing race: year={year}, round={round_num}")
            continue
        seen_races.add(race_key)

        circuit_ref = value["Circuit"]["circuitId"]
        circuit_id = circuit_map.get(circuit_ref)
        if not circuit_id:
            circuit_transformed.append({
                "circuitid": next_circuit_id,
                "circuitref": circuit_ref,
                "name_y": value["Circuit"]["circuitName"],
                "url_y": value["Circuit"]["url"],
                "lat": int(float(value["Circuit"]["Location"]["lat"])),
                "lng": int(float(value["Circuit"]["Location"]["long"])),
                "country": value["Circuit"]["Location"]["country"],
                "location": value["Circuit"]["Location"]["locality"]
            })
            circuit_id = next_circuit_id
            circuit_map[circuit_ref] = circuit_id
            next_circuit_id += 1

        time_races = value.get("time", None)
        if time_races:
            time_races = time_races.replace("Z", "")

        race_transformed.append({
            "race_id": next_race_id,
            "circuitid": circuit_id,
            "year": year,
            "round": round_num,
            "date": value["date"],
            "time_races": time_races,
            "name_x": value["raceName"],
            "url_x": value["url"]
        })

        fp_data = {}
        if "FirstPractice" in value and "time" in value["FirstPractice"]:
            fp_data["race_id"] = next_race_id
            fp_data["fp1_date"] = value["FirstPractice"]["date"]
            fp_data["fp1_time"] = value["FirstPractice"]["time"].replace("Z", "")
        if "SecondPractice" in value and "time" in value["SecondPractice"]:
            fp_data["fp2_date"] = value["SecondPractice"]["date"]
            fp_data["fp2_time"] = value["SecondPractice"]["time"].replace("Z", "")
        if "ThirdPractice" in value and "time" in value["ThirdPractice"]:
            fp_data["fp3_date"] = value["ThirdPractice"]["date"]
            fp_data["fp3_time"] = value["ThirdPractice"]["time"].replace("Z", "")
        if fp_data:
            fp_transformed.append(fp_data)

        if "Qualifying" in value and "time" in value["Qualifying"]:
            quali_transformed.append({
                "race_id": next_race_id,
                "quali_date": value["Qualifying"]["date"],
                "quali_time": value["Qualifying"]["time"].replace("Z", "")
            })

        sprint_data = {"race_id": next_race_id}
        if "Sprint" in value and "time" in value["Sprint"]:
            sprint_data["sprint_date"] = value["Sprint"]["date"]
            sprint_data["sprint_time"] = value["Sprint"]["time"].replace("Z", "")
            logging.info(f"Found sprint for race_id={next_race_id}, year={year}, round={round_num}: {sprint_data['sprint_date']} {sprint_data['sprint_time']}")
        else:
            sprint_data["sprint_date"] = None
            sprint_data["sprint_time"] = None
            logging.debug(f"No sprint for race_id={next_race_id}, year={year}, round={round_num}")
        sprint_transformed.append(sprint_data)

        next_race_id += 1

    if circuit_transformed:
        circuit_rows = [(r["circuitid"], r["circuitref"], r["name_y"], r["url_y"], r["lat"], r["lng"], r["country"], r["location"])
                        for r in circuit_transformed]
        hook.insert_rows(table="circuit", rows=circuit_rows, target_fields=["circuitid", "circuitref", "name_y", "url_y", "lat", "lng", "country", "location"])

    if race_transformed:
        race_rows = [(r["race_id"], r["circuitid"], r["year"], r["round"], None, None, None, None, r["date"], r["time_races"], r["name_x"], r["url_x"])
                     for r in race_transformed]
        hook.insert_rows(table="race", rows=race_rows, target_fields=["race_id", "circuitid", "year", "round", "laps", "fastestlap", "fastestlaptime", "fastestlapspeed", "date", "time_races", "name_x", "url_x"])
        print("New data inserted into race table:")
        for row in race_rows:
            print(f"race_id={row[0]}, year={row[2]}, round={row[3]}, name_x={row[10]}")

    if fp_transformed:
        fp_rows = [(r["race_id"], r.get("fp1_date"), r.get("fp1_time"), r.get("fp2_date"), r.get("fp2_time"), r.get("fp3_date"), r.get("fp3_time"))
                   for r in fp_transformed]
        hook.insert_rows(table="freepractice", rows=fp_rows, target_fields=["race_id", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time"])

    if quali_transformed:
        quali_rows = [(r["race_id"], r["quali_date"], r["quali_time"]) for r in quali_transformed]
        hook.insert_rows(table="qualifying", rows=quali_rows, target_fields=["race_id", "quali_date", "quali_time"])

    if sprint_transformed:
        sprint_rows = [(r["race_id"], r["sprint_date"], r["sprint_time"]) for r in sprint_transformed]
        hook.insert_rows(table="sprint", rows=sprint_rows, target_fields=["race_id", "sprint_date", "sprint_time"])
        logging.info(f"Inserted {len(sprint_rows)} new sprint records")

    return "Race data processed and loaded."

@task
def process_constructor_data(messages):
    """Transform and load constructor data from Kafka into PostgreSQL."""
    if not messages:
        logging.info("No constructor messages to process")
        return "No data processed."

    hook = PostgresHook(postgres_conn_id="formula_one_connection")
    existing_refs_query = "SELECT constructorRef FROM constructor;"
    existing_refs = {row[0] for row in hook.get_records(existing_refs_query)}
    max_id_query = "SELECT MAX(constructorId) FROM constructor;"
    max_constructor_id = hook.get_first(max_id_query)[0] or 0
    next_constructor_id = max_constructor_id + 1

    transformed = []
    seen_constructor_refs = set()
    for msg in messages:
        constructor_ref = msg["value"].get("constructorId", msg["key"])
        if constructor_ref in existing_refs or constructor_ref in seen_constructor_refs:
            logging.info(f"Skipping existing constructorRef: {constructor_ref}")
            continue
        seen_constructor_refs.add(constructor_ref)

        transformed.append({
            "constructorId": next_constructor_id,
            "constructorRef": constructor_ref,
            "name": msg["value"].get("name", "Unknown"),
            "nationality_constructors": msg["value"].get("nationality"),
            "url_constructors": msg["value"].get("url")
        })
        next_constructor_id += 1

    if transformed:
        rows_to_insert = [(r["constructorId"], r["constructorRef"], r["name"], r["nationality_constructors"], r["url_constructors"])
                         for r in transformed]
        hook.insert_rows(table="constructor", rows=rows_to_insert,
                         target_fields=["constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors"])
        print("New data inserted into constructor table:")
        for row in rows_to_insert:
            print(f"constructorId={row[0]}, constructorRef={row[1]}, name={row[2]}")
        logging.info(f"Inserted {len(rows_to_insert)} new constructor records")
    return "Constructor data processed and loaded."

@task
def update_race_fastest_laps():
    """Update the race table with fastest lap info from lapTimes for 2024 races."""
    hook = PostgresHook(postgres_conn_id="formula_one_connection")

    race_query = "SELECT race_id FROM race WHERE year = 2024;"
    races_2024 = hook.get_records(race_query)

    for (race_id,) in races_2024:
        fastest_lap_query = """
            SELECT lap, time_laptimes
            FROM lapTimes
            WHERE race_id = %s
            ORDER BY milliseconds_laptimes ASC
            LIMIT 1;
        """
        fastest_lap = hook.get_first(fastest_lap_query, parameters=(race_id,))
        
        if fastest_lap:
            lap_number, lap_time = fastest_lap
            update_query = """
                UPDATE race
                SET fastestLap = %s,
                    fastestLapTime = %s,
                    fastestLapSpeed = NULL  -- Requires circuit length
                WHERE race_id = %s;
            """
            hook.run(update_query, parameters=(lap_number, lap_time, race_id))
            logging.info(f"Updated race_id={race_id}: fastestLap={lap_number}, fastestLapTime={lap_time}, fastestLapSpeed=NULL")
        else:
            logging.info(f"No lap times found for race_id={race_id}, skipping fastest lap update")

    return "Race table updated with fastest lap info."

@dag(
    dag_id="kafka_consumer_processor_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
)
def kafka_consumer_dag():
    """DAG to consume, transform, and load Kafka messages into PostgreSQL, then update race table."""
    consume_status = consume_from_topic.override(task_id="consume_status")("status_topic")
    process_status = process_status_data.override(task_id="process_status")(consume_status)

    consume_driver = consume_from_topic.override(task_id="consume_driver")("driver_topic")
    process_driver = process_driver_data.override(task_id="process_driver")(consume_driver)

    consume_race = consume_from_topic.override(task_id="consume_race")("race_topic")
    process_race = process_race_data.override(task_id="process_race")(consume_race)

    consume_constructor = consume_from_topic.override(task_id="consume_constructor")("constructor_topic")
    process_constructor = process_constructor_data.override(task_id="process_constructor")(consume_constructor)

    consume_laptimes = consume_from_topic.override(task_id="consume_laptimes")("laptimes_topic")
    process_laptimes = process_laptimes_data.override(task_id="process_laptimes")(consume_laptimes)

    consume_driverstandings = consume_from_topic.override(task_id="consume_driverstandings")("driverstandings_topic")
    process_driverstandings = process_driverstandings_data.override(task_id="process_driverstandings")(consume_driverstandings)

    consume_constandings = consume_from_topic.override(task_id="consume_constandings")("constandings_topic")
    process_constandings = process_constandings_data.override(task_id="process_constandings")(consume_constandings)

    consume_pitstops = consume_from_topic.override(task_id="consume_pitstops")("pitstops_topic")
    process_pitstops = process_pitstops_data.override(task_id="process_pitstops")(consume_pitstops)

    consume_results = consume_from_topic.override(task_id="consume_results")("results_topic")
    process_results = process_results_data.override(task_id="process_results")(consume_results)

    update_race = update_race_fastest_laps.override(task_id="update_race_fastest_laps")()

    # Set task dependencies
    consume_status >> process_status
    consume_driver >> process_driver
    consume_race >> process_race
    consume_constructor >> process_constructor
    consume_laptimes >> process_laptimes >> update_race
    consume_driverstandings >> process_driverstandings
    consume_constandings >> process_constandings
    consume_pitstops >> process_pitstops
    consume_results >> process_results

# Instantiate the DAG
kafka_consumer_dag() 