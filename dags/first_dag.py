import os
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task, dag
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

data_path = "/opt/airflow/data/"

constructor_parquet_file = os.path.join(data_path, "constructor.parquet")
conStandings_parquet_file = os.path.join(data_path, "constandings.parquet")
pitStops_parquet_file = os.path.join(data_path, "pitstops.parquet")
lapTimes_parquet_file = os.path.join(data_path, "laptimes.parquet")
driverStandings_parquet_file = os.path.join(data_path, "driverstandings.parquet")
driver_parquet_file = os.path.join(data_path, "driver.parquet")
formulaone_parquet_file = os.path.join(data_path, "formulaone.parquet")
sprint_parquet_file = os.path.join(data_path, "sprint.parquet")
circuit_parquet_file = os.path.join(data_path, "circuit.parquet")
qualifying_parquet_file = os.path.join(data_path, "qualifying.parquet")
freePractice_parquet_file = os.path.join(data_path, "freepractice.parquet")
race_parquet_file = os.path.join(data_path, "race.parquet")

transformed_file = os.path.join(data_path, "transformed_data.parquet")


@dag(schedule_interval=None, start_date=days_ago(1), default_args=default_args, catchup=False)
def csv_to_postgres():

#region TRUNCATE_TABLES
    @task
    def truncate_all_tables():
        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        truncate_sql = """
        SELECT 'TRUNCATE TABLE public.' || table_name || ' CASCADE;' 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
        """
        truncate_statements = hook.get_records(truncate_sql)
        for stmt in truncate_statements:
            hook.run(stmt[0])
        print("Svi podaci u bazi su obrisani.")
#endregion

#region EXTRACT
    @task(multiple_outputs=True)
    def extract_to_parquet():
        print("Uslo u extract")
        file_path = os.path.join(data_path, "dataEngineeringDataset.csv")
        if os.path.exists(file_path):
            print("Pokusaj iscitavanja fajla")
            df = pd.read_csv(file_path)
            print("iscitan fajl")

            #STATUS
            status_parquet_file = os.path.join(data_path, "status.parquet")
            status_df = df[["statusId", "status"]].drop_duplicates()
            table_status = pa.Table.from_pandas(status_df, preserve_index=False)
            pq.write_table(table_status, os.path.join(data_path, "status.parquet"), coerce_timestamps='ms', allow_truncated_timestamps=True)

            #CONSTRUCTOR STANDINGS
            constandings_parquet_file = os.path.join(data_path, "constandings.parquet")
            constandings_df = df[["constructorStandingsId", "points_constructorstandings", "position_constructorstandings", "positionText_constructorstandings", "wins_constructorstandings"]].drop_duplicates()
            table_constandings = pa.Table.from_pandas(constandings_df, preserve_index=False)
            pq.write_table(table_constandings, os.path.join(data_path, "constandings.parquet"), coerce_timestamps='ms', allow_truncated_timestamps=True)

            # DRIVER STANDINGS
            driverstandings_parquet_file = os.path.join(data_path, "driverstandings.parquet")
            driverstandings_df = df[["driverStandingsId", "points_driverstandings", "position_driverstandings", "positionText_driverstandings"]].drop_duplicates()
            table_driverstandings = pa.Table.from_pandas(driverstandings_df, preserve_index=False)
            pq.write_table(table_driverstandings, driverstandings_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # DRIVER
            print("Pravljenje parket fajla za vozaca...")
            df['number_drivers'] = pd.to_numeric(df['number_drivers'], errors='coerce')  # Coerce pretvara nevalidne vrednosti u NaN
            driver_parquet_file = os.path.join(data_path, "driver.parquet")
            driver_df = df[["driverId", "driverRef", "number_drivers", "code", "forename", "surname", "dob", "nationality", "url"]].drop_duplicates()
            table_driver = pa.Table.from_pandas(driver_df, preserve_index=False)
            pq.write_table(table_driver, driver_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # CONSTRUCTOR
            constructor_parquet_file = os.path.join(data_path, "constructor.parquet")
            constructor_df = df[["constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors"]].drop_duplicates()
            table_constructor = pa.Table.from_pandas(constructor_df, preserve_index=False)
            pq.write_table(table_constructor, constructor_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # PITSTOPS
            pitstops_parquet_file = os.path.join(data_path, "pitstops.parquet")
            pitstops_df = df[["stop", "raceId", "driverId", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops"]].drop_duplicates()
            pitstops_df['duration'] = pd.to_numeric(pitstops_df['duration'], errors='coerce')
            table_pitstops = pa.Table.from_pandas(pitstops_df, preserve_index=False)
            pq.write_table(table_pitstops, pitstops_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # LAPTIMES
            laptimes_parquet_file = os.path.join(data_path, "laptimes.parquet")
            laptimes_df = df[["raceId", "driverId", "lap", "position_laptimes", "time_laptimes", "milliseconds_laptimes"]].drop_duplicates()
            #laptimes_df['milliseconds_laptimes'] = pd.to_numeric(laptimes_df['milliseconds_laptimes'], errors='coerce')
            table_laptimes = pa.Table.from_pandas(laptimes_df, preserve_index=False)
            pq.write_table(table_laptimes, laptimes_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # QUALIFYING
            qualifying_parquet_file = os.path.join(data_path, "qualifying.parquet")
            qualifying_df = df[["raceId", "quali_date", "quali_time"]].drop_duplicates()
            table_qualifying = pa.Table.from_pandas(qualifying_df, preserve_index=False)
            pq.write_table(table_qualifying, qualifying_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # FREE PRACTICE
            print("Pravljenje parket fajla za slobodne treninge...")
            freePractice_parquet_file = os.path.join(data_path, "freePractice.parquet")
            freePractice_df = df[["raceId", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time"]].drop_duplicates()
            table_freePractice = pa.Table.from_pandas(freePractice_df, preserve_index=False)
            pq.write_table(table_freePractice, freePractice_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # SPRINT
            print("Pravljenje parket fajla za sprint...")
            sprint_parquet_file = os.path.join(data_path, "sprint.parquet")
            sprint_df = df[["raceId", "sprint_date", "sprint_time"]].drop_duplicates()
            table_sprint = pa.Table.from_pandas(sprint_df, preserve_index=False)
            pq.write_table(table_sprint, sprint_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # CIRCUIT
            circuit_parquet_file = os.path.join(data_path, "circuit.parquet")
            circuit_df = df[["circuitId", "circuitRef", "name_y", "location", "country", "lat", "lng", "alt", "url_y"]].drop_duplicates()
            circuit_df['lat'] = pd.to_numeric(circuit_df['lat'], errors='coerce')
            circuit_df['lng'] = pd.to_numeric(circuit_df['lng'], errors='coerce')
            circuit_df['alt'] = pd.to_numeric(circuit_df['alt'], errors='coerce')
            table_circuit = pa.Table.from_pandas(circuit_df, preserve_index=False)
            pq.write_table(table_circuit, circuit_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # RACE
            print("Pravljenje parket fajla za race...")
            race_parquet_file = os.path.join(data_path, "race.parquet")
            race_df = df[[
                "raceId", "circuitId", "year", "round", "laps", "fastestLap","fastestLapTime", "fastestLapSpeed", "name_x", "date", "time_races", "url_x"
            ]].drop_duplicates()
            race_df['fastestLap'] = pd.to_numeric(race_df['fastestLap'], errors='coerce')
            race_df['fastestLapTime'] = pd.to_datetime(race_df['fastestLapTime'], errors='coerce')
            race_df['fastestLapSpeed'] = pd.to_numeric(race_df['fastestLapSpeed'], errors='coerce')
            race_df['date'] = pd.to_datetime(race_df['date'], errors='coerce')
            race_df['time_races'] = pd.to_datetime(race_df['time_races'], errors='coerce')
            table_race = pa.Table.from_pandas(race_df, preserve_index=False)
            pq.write_table(table_race, race_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

            # FORMULA ONE
            formulaone_parquet_file = os.path.join(data_path, "formulaone.parquet")
            formulaone_df = df[[
                "resultId", "raceId", "driverId", "constructorId", "statusId", "driverStandingsId",
                "constructorStandingsId", "grid", "stop", "position", "rank", "time", "milliseconds",
                "points", "number_drivers","number", "lap", "positionOrder", "positionText", "wins"
            ]].drop_duplicates()
            formulaone_df['grid'] = pd.to_numeric(formulaone_df['grid'], errors='coerce')
            formulaone_df['stop'] = pd.to_numeric(formulaone_df['stop'], errors='coerce')
            formulaone_df['rank'] = pd.to_numeric(formulaone_df['rank'], errors='coerce')
            formulaone_df['number'] = pd.to_numeric(formulaone_df['number'], errors='coerce')
            formulaone_df['milliseconds'] = pd.to_numeric(formulaone_df['milliseconds'], errors='coerce')
            formulaone_df['points'] = pd.to_numeric(formulaone_df['points'], errors='coerce')
            formulaone_df['number_drivers'] = pd.to_numeric(formulaone_df['number_drivers'], errors='coerce')
            formulaone_df['lap'] = pd.to_numeric(formulaone_df['lap'], errors='coerce')
            formulaone_df['positionOrder'] = pd.to_numeric(formulaone_df['positionOrder'], errors='coerce')
            formulaone_df['positionText'] = pd.to_numeric(formulaone_df['positionText'], errors='coerce')
            formulaone_df['position'] = pd.to_numeric(formulaone_df['position'], errors='coerce')
            formulaone_df['wins'] = pd.to_numeric(formulaone_df['wins'], errors='coerce')
            #formulaone_df['time'] = pd.to_datetime(formulaone_df['time'], errors='coerce')
            table_formulaone = pa.Table.from_pandas(formulaone_df, preserve_index=False)
            pq.write_table(table_formulaone, formulaone_parquet_file, coerce_timestamps='ms', allow_truncated_timestamps=True)


            return {
                "status" : status_parquet_file,
                "constandings": constandings_parquet_file,
                "driverstandings": driverstandings_parquet_file,
                "driver": driver_parquet_file,
                "constructor": constructor_parquet_file,
                "pitstops": pitstops_parquet_file,
                "laptimes": laptimes_parquet_file,
                "qualifying": qualifying_parquet_file,
                "freePractice": freePractice_parquet_file,
                "sprint": sprint_parquet_file,
                "circuit": circuit_parquet_file,
                "race": race_parquet_file,
                "formula_one": formulaone_parquet_file
            }
        else:
            raise FileNotFoundError(f"Fajl nije pronađen: {file_path}")
#endregion

#region TRANSFORM
    @task
    def transform_formula_one_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["resultId"]).copy()

        columns_to_convert = ["milliseconds", "rank", "number", "points", "wins", "grid", "lap", "positionOrder", "positionText"]
        for col in columns_to_convert:
            if col in table_unique.columns:
                table_unique.loc[:, col] = pd.to_numeric(table_unique[col], errors='coerce')


        expected_columns = [
            "resultId", "raceId", "driverId", "constructorId", "statusId", "driverStandingsId",
            "constructorStandingsId", "grid", "stop", "position", "rank", "time", "milliseconds", "points",
            "number", "lap", "positionOrder", "positionText", "wins"
        ]
        
        available_columns = [col for col in expected_columns if col in table_unique.columns]
        missing_columns = set(expected_columns) - set(available_columns)

        if missing_columns:
            print(f"Upozorenje: Nedostaju sledeće kolone u tabeli: {missing_columns}")

        transformed_formula_one_data = table_unique[available_columns].to_dict(orient='records')

        transformed_formula_one_file = os.path.join(data_path, "transformed_formula_one.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_formula_one_data), preserve_index=False)
        pq.write_table(new_table, transformed_formula_one_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_formula_one_file}")
        return transformed_formula_one_file




    @task
    def transform_circuit_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["circuitId"])

        table_unique["lat"] = pd.to_numeric(table_unique["lat"], errors='coerce') 
        table_unique["lng"] = pd.to_numeric(table_unique["lng"], errors='coerce')
        table_unique["alt"] = pd.to_numeric(table_unique["alt"], errors='coerce')

        transformed_circuit_data = table_unique[[
            "circuitId", "circuitRef", "name_y", "location", "country", "lat", "lng", "alt", "url_y"
        ]].to_dict(orient='records')

        transformed_circuit_file = os.path.join(data_path, "transformed_circuit.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_circuit_data), preserve_index=False)
        pq.write_table(new_table, transformed_circuit_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_circuit_file}")

        return transformed_circuit_file
    
    @task
    def transform_race_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["raceId"])

        table_unique["fastestLapTime"] = pd.to_datetime(table_unique["fastestLapTime"], errors='coerce')
        table_unique["fastestLapSpeed"] = pd.to_numeric(table_unique["fastestLapSpeed"], errors='coerce')
        table_unique["date"] = pd.to_datetime(table_unique["date"], errors='coerce')
        table_unique["time_races"] = pd.to_datetime(table_unique["time_races"], errors='coerce')

        table_unique["year"] = pd.to_numeric(table_unique["year"], errors='coerce') 
        table_unique["round"] = pd.to_numeric(table_unique["round"], errors='coerce')
        table_unique["laps"] = pd.to_numeric(table_unique["laps"], errors='coerce')
        table_unique["fastestLap"] = pd.to_numeric(table_unique["fastestLap"], errors='coerce')

        transformed_race_data = table_unique[[
            "raceId", "circuitId", "year", "round", "laps", "fastestLap", 
            "fastestLapTime", "fastestLapSpeed", "name_x", "date", "time_races", "url_x"
        ]].to_dict(orient='records')

        transformed_race_file = os.path.join(data_path, "transformed_race.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_race_data), preserve_index=False)
        pq.write_table(new_table, transformed_race_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_race_file}")
        return transformed_race_file


    @task
    def transform_sprint_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["raceId"])

        table_unique["sprint_date"] = table_unique["sprint_date"].replace({"\\N": None})
        table_unique["sprint_time"] = table_unique["sprint_time"].replace({"\\N": None})

        transformed_sprint_data = table_unique[[
            "raceId", "sprint_date", "sprint_time"
        ]].to_dict(orient='records')

        transformed_sprint_file = os.path.join(data_path, "transformed_sprint.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_sprint_data), preserve_index=False)
        pq.write_table(new_table, transformed_sprint_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_sprint_file}")

        return transformed_sprint_file


    @task
    def transform_freePractice_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["raceId"])

        table_unique["fp1_date"] = table_unique["fp1_date"].replace({"\\N": None})
        table_unique["fp1_time"] = table_unique["fp1_time"].replace({"\\N": None})
        table_unique["fp2_date"] = table_unique["fp2_date"].replace({"\\N": None})
        table_unique["fp2_time"] = table_unique["fp2_time"].replace({"\\N": None})
        table_unique["fp3_date"] = table_unique["fp3_date"].replace({"\\N": None})
        table_unique["fp3_time"] = table_unique["fp3_time"].replace({"\\N": None})

        transformed_freePractice_data = table_unique[[
            "raceId", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time"
        ]].to_dict(orient='records')

        transformed_freePractice_file = os.path.join(data_path, "transformed_freePractice.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_freePractice_data), preserve_index=False)
        pq.write_table(new_table, transformed_freePractice_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_freePractice_file}")

        return transformed_freePractice_file


    @task
    def transform_qualifying_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["raceId"])

        table_unique["quali_date"] = table_unique["quali_date"].replace({"\\N": None})
        table_unique["quali_time"] = table_unique["quali_time"].replace({"\\N": None})


        transformed_qualifying_data = table_unique[[
            "raceId", "quali_date", "quali_time"
        ]].to_dict(orient='records')

        transformed_qualifying_file = os.path.join(data_path, "transformed_qualifying.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_qualifying_data), preserve_index=False)
        pq.write_table(new_table, transformed_qualifying_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_qualifying_file}")

        return transformed_qualifying_file


    @task
    def transform_laptimes_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")

        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["lap", "raceId", "driverId"])

        transformed_laptimes_data = table_unique[[
            "lap", "raceId", "driverId", "time_laptimes", "milliseconds_laptimes", "position_laptimes"
        ]].to_dict(orient='records')

        transformed_laptimes_file = os.path.join(data_path, "transformed_laptimes.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_laptimes_data), preserve_index=False)

        pq.write_table(new_table, transformed_laptimes_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_laptimes_file}")

        return transformed_laptimes_file
    

    @task
    def transform_pitstops_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")
        
        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates(subset=["stop", "raceId", "driverId"])

        transformed_pitstops_data = [{
            "stop": row["stop"],
            "raceId": row["raceId"],
            "driverId": row["driverId"],
            "lap_pitstops": row["lap_pitstops"],
            "time_pitstops": row["time_pitstops"],
            "duration": row["duration"],
            "milliseconds_pitstops": row["milliseconds_pitstops"]
        } for _, row in table_unique.iterrows()]

        transformed_pitstops_file = os.path.join(data_path, "transformed_pitstops.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_pitstops_data), preserve_index=False)
        pq.write_table(new_table, transformed_pitstops_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_pitstops_file}")
        
        return transformed_pitstops_file


    @task
    def transform_constructor_data(input_parquet: str):
        print(f"Transformisanje podataka iz fajla: {input_parquet}")
        
        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates()

        transformed_constructor_data = [{
            "constructorId": row["constructorId"],
            "constructorRef": row["constructorRef"],
            "name": row["name"],
            "nationality_constructors": row["nationality_constructors"],
            "url_constructors": row["url_constructors"]
        } for _, row in table_unique.iterrows()]

        transformed_constructor_file = os.path.join(data_path, "transformed_constructor.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_constructor_data), preserve_index=False)
        pq.write_table(new_table, transformed_constructor_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        print(f"Podaci uspešno transformisani i sačuvani u: {transformed_constructor_file}")
        
        return transformed_constructor_file


    @task
    def transform_driver_data(input_parquet: str):
        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates()

        transformed_driver_data = [{
            "driverId": row["driverId"],
            "driverRef": row["driverRef"],
            "number_drivers": row["number_drivers"],
            "code": row["code"],
            "forename": row["forename"],
            "surname": row["surname"],
            "dob": row["dob"],
            "nationality": row["nationality"],
            "url": row["url"]
        } for _, row in table_unique.iterrows()]

        transformed_driver_file = os.path.join(data_path, "transformed_driver.parquet")

        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_driver_data), preserve_index=False)
        pq.write_table(new_table, transformed_driver_file, coerce_timestamps='ms', allow_truncated_timestamps=True)

        return transformed_driver_file

    @task
    def transform_driverstandings_data(input_parquet: str):
        table = pq.read_pandas(input_parquet).to_pandas()

        table_unique = table.drop_duplicates()
        
        transformed_driverstandings = [{
            "driverStandingsId": row["driverStandingsId"],
            "points_driverstandings": row["points_driverstandings"],
            "position_driverstandings": row["position_driverstandings"],
            "positionText_driverstandings": row["positionText_driverstandings"]
        } for _, row in table_unique.iterrows()]

        transformed_driverstandings_file = os.path.join(data_path, "transformed_driverstandings.parquet")
        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_driverstandings), preserve_index=False)
        pq.write_table(new_table, transformed_driverstandings_file, coerce_timestamps='ms', allow_truncated_timestamps=True)
        
        return transformed_driverstandings_file

    @task
    def transform_constandings_data(input_parquet: str):
        table = pq.read_pandas(input_parquet).to_pandas()
        
        table_unique = table.drop_duplicates()
        transformed_constandings = [{
            "constandingsId": row["constructorStandingsId"],
            "points_constructorstandings": row["points_constructorstandings"],
            "position_constructorstandings": row["position_constructorstandings"],
            "positionText_constructorstandings": row["positionText_constructorstandings"],
            "wins_constructorstandings": row["wins_constructorstandings"]
        } for _, row in table_unique.iterrows()]

        transformed_constandings_file = os.path.join(data_path, "transformed_constandings.parquet")
        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_constandings), preserve_index=False)
        pq.write_table(new_table, transformed_constandings_file, coerce_timestamps='ms', allow_truncated_timestamps=True)
        return transformed_constandings_file
    

    @task
    def transform_status_data(input_parquet: str):
        table = pq.read_pandas(input_parquet).to_pandas()
        
        table_unique = table.drop_duplicates()
        transformed_data = [{"statusId": int(row["statusId"]), "status_name": row["status"]} for _, row in table_unique.iterrows()]
        
        transformed_status_file = os.path.join(data_path, "transformed_status.parquet")
        new_table = pa.Table.from_pandas(pd.DataFrame(transformed_data), preserve_index=False)
        pq.write_table(new_table, transformed_status_file, coerce_timestamps='ms', allow_truncated_timestamps=True)
        return transformed_status_file
#endregion    

#region LOAD
    @task
    def load_formula_one_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['position'] = table['position'].fillna(0)
        table['stop'] = table['stop'].fillna(0)
        table['grid'] = table['grid'].fillna(0)
        table['number'] = table['number'].fillna(0)
        table['rank'] = table['rank'].fillna(0)
        table['milliseconds'] = table['milliseconds'].fillna(0)


        table['lap'] = table['lap'].fillna(0)
        table['positionOrder'] = table['positionOrder'].fillna(0)
        table['positionText'] = table['positionText'].fillna(0)

        rows_to_insert = [
            (
                row['resultId'],
                row['raceId'],
                row['driverId'],
                row['constructorId'],
                row['statusId'],
                row['driverStandingsId'],
                row['constructorStandingsId'],
                row['grid'],
                row['stop'],
                row['position'],
                row['rank'],
                row['time'],
                row['milliseconds'],
                row['points'],
                row['number'],
                row['lap'],
                row['positionOrder'],
                row['positionText'],
                row['wins']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="FormulaOne",
            rows=rows_to_insert,
            target_fields=[
                "resultId", "raceId", "driverId", "constructorId", "statusId", 
                "driverStandingsId", "conStandingsId", "grid", "stop", "position", 
                "rank", "time", "milliseconds", "points", "number", "lap", 
                "positionOrder", "positionText", "wins"
            ]
        )
        return "Podaci uspešno uneti u tabelu FormulaOne."


    @task
    def load_race_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['fastestLapTime'] = table['fastestLapTime'].apply(lambda x: None if pd.isna(x) else x.time())
        table['date'] = table['date'].apply(lambda x: None if pd.isna(x) else x)
        table['time_races'] = table['time_races'].apply(lambda x: None if pd.isna(x) else x.time())

        table['year'] = table['year'].fillna(0)
        table['round'] = table['round'].fillna(0)
        table['laps'] = table['laps'].fillna(0)
        table['fastestLapSpeed'] = table['fastestLapSpeed'].fillna(0)

        rows_to_insert = [
            (
                row['raceId'],
                row['circuitId'],
                row['year'],
                row['round'],
                row['laps'],
                row['fastestLap'],
                row['fastestLapTime'],
                row['fastestLapSpeed'],
                row['name_x'],
                row['date'],
                row['time_races'],
                row['url_x']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="race",
            rows=rows_to_insert,
            target_fields=["race_id", "circuitId", "year", "round", "laps", "fastestLap", "fastestLapTime", "fastestLapSpeed", "name_x", "date", "time_races", "url_x"]
        )
        return "Podaci uspešno uneti u tabelu race."


    @task
    def load_circuit_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['lat'] = table['lat'].fillna(0)
        table['lng'] = table['lng'].fillna(0)
        table['alt'] = table['alt'].fillna(0)

        rows_to_insert = [
            (
                row['circuitId'],
                row['circuitRef'],
                row['name_y'],
                row['location'],
                row['country'],
                row['lat'],
                row['lng'],
                row['alt'],
                row['url_y']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="circuit",
            rows=rows_to_insert,
            target_fields=["circuitId", "circuitRef", "name_y", "location", "country", "lat", "lng", "alt", "url_y"]
        )
        return "Podaci uspešno uneti u tabelu circuit."


    @task
    def load_sprint_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['sprint_date'] = table['sprint_date'].apply(lambda x: None if x == "\\N" else x)
        table['sprint_time'] = table['sprint_time'].apply(lambda x: None if x == "\\N" else x)

        rows_to_insert = [
            (
                row['raceId'],
                row['sprint_date'],
                row['sprint_time']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="sprint",
            rows=rows_to_insert,
            target_fields=["race_id", "sprint_date", "sprint_time"]
        )
        return "Podaci uspešno uneti u tabelu sprint."


    @task
    def load_freePractice_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['fp1_date'] = table['fp1_date'].apply(lambda x: None if x == "\\N" else x)
        table['fp1_time'] = table['fp1_time'].apply(lambda x: None if x == "\\N" else x)
        table['fp2_date'] = table['fp2_date'].apply(lambda x: None if x == "\\N" else x)
        table['fp2_time'] = table['fp2_time'].apply(lambda x: None if x == "\\N" else x)
        table['fp3_date'] = table['fp3_date'].apply(lambda x: None if x == "\\N" else x)
        table['fp3_time'] = table['fp3_time'].apply(lambda x: None if x == "\\N" else x)

        rows_to_insert = [
            (
                row['raceId'],
                row['fp1_date'],
                row['fp1_time'],
                row['fp2_date'],
                row['fp2_time'],
                row['fp3_date'],
                row['fp3_time']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="freePractice",
            rows=rows_to_insert,
            target_fields=["race_id", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time"]
        )
        return "Podaci uspešno uneti u tabelu freePractice."


    @task
    def load_qualifying_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['quali_date'] = table['quali_date'].apply(lambda x: None if x == "\\N" else x)
        table['quali_time'] = table['quali_time'].apply(lambda x: None if x == "\\N" else x)

        rows_to_insert = [
            (
                row['raceId'],
                row['quali_date'],
                row['quali_time']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="qualifying",
            rows=rows_to_insert,
            target_fields=["race_id", "quali_date", "quali_time"]
        )
        return "Podaci uspešno uneti u tabelu qualifying."


    @task
    def load_laptimes_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['lap'] = table['lap'].fillna(0)
        table['position_laptimes'] = table['position_laptimes'].fillna(0)
        table['time_laptimes'] = table['time_laptimes'].fillna(0)
        table['milliseconds_laptimes'] = table['milliseconds_laptimes'].fillna(0)

        rows_to_insert = [
            (
                row['raceId'],
                row['driverId'],
                row['lap'],
                row['position_laptimes'],
                row['time_laptimes'],
                row['milliseconds_laptimes']
            ) for _, row in table.iterrows()
        ]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        
        hook.insert_rows(
            table="laptimes",
            rows=rows_to_insert,
            target_fields=["race_id", "driverId", "lap", "position_laptimes", "time_laptimes", "milliseconds_laptimes"]
        )
        return "Podaci uspešno uneti u tabelu laptimes."


    @task
    def load_pitstops_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        table['stop'] = table['stop'].apply(lambda x: None if pd.isna(x) else x)
        table['lap_pitstops'] = table['lap_pitstops'].fillna(0)
        table['duration'] = table['duration'].fillna(0)
        table['milliseconds_pitstops'] = table['milliseconds_pitstops'].fillna(0)

        #CSV names
        rows_to_insert = [(
            row['stop'],
            row['raceId'],
            row['driverId'],
            row['lap_pitstops'],
            row['time_pitstops'],
            row['duration'],
            row['milliseconds_pitstops']
        ) for _, row in table.iterrows()]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        #DB names
        hook.insert_rows(
            table="pitstops",
            rows=rows_to_insert, 
            target_fields=["stop", "race_id", "driverId", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops"]
        )
        return "Podaci uspešno uneti u tabelu pitstops."


    @task
    def load_constructor_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")
        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        rows_to_insert = [(
            row['constructorId'],
            row['constructorRef'],
            row['name'],
            row['nationality_constructors'],
            row['url_constructors']
        ) for _, row in table.iterrows()]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        hook.insert_rows(
            table="constructor",
            rows=rows_to_insert, 
            target_fields=["constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors"]
        )
        return "Podaci uspešno uneti u tabelu constructor."


    @task
    def load_driver_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)
        
        print("PRE rows_to_insert")

        table = table.fillna({
            'number_drivers': 0
        })

        rows_to_insert = [(
            row['driverId'],
            row['driverRef'],
            row['number_drivers'],
            row['code'],
            row['forename'],
            row['surname'],
            row['dob'],
            row['nationality'],
            row['url'])
            for _, row in table.iterrows()]
        
        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        hook.insert_rows(
            table="driver",
            rows=rows_to_insert, 
            target_fields=["driverId", "driverRef", "number_drivers", "code", "forename", "surname", "dob", "nationality", "url"]
        )
        return "Podaci uspešno uneti u tabelu driver."
    
    @task
    def load_driverstandings_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        rows_to_insert = [(
            row['driverStandingsId'],
            row['points_driverstandings'],
            row['position_driverstandings'],
            row['positionText_driverstandings'])
            for _, row in table.iterrows()]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")
        hook.insert_rows(
            table="driverstandings", 
            rows=rows_to_insert, 
            target_fields=["driverStandingsId", "points_driverstandings", "position_driverstandings", "positionText_driverstandings"]
        )
        return "Podaci uspešno uneti u tabelu driverstandings."

    @task
    def load_constandings_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")

        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)

        rows_to_insert = [(
            row['constandingsId'],
            row['points_constructorstandings'],
            row['position_constructorstandings'],
            row['positionText_constructorstandings'],
            row['wins_constructorstandings']) 
            for _, row in table.iterrows()]

        hook = PostgresHook(postgres_conn_id="formula_one_connection")

        hook.insert_rows(table="constandings", rows=rows_to_insert, 
        target_fields=[
            "constandingsId",
            "points_constructorstandings", 
            "position_constructorstandings", 
            "positionText_constructorstandings", 
            "wins_constructorstandings"])

        return "Podaci uspešno uneti u tabelu constandings."


    @task
    def load_status_data(transformed_parquet: str):
        print(f"Učitavam fajl: {transformed_parquet}")
    
        if not os.path.exists(transformed_parquet):
            raise FileNotFoundError(f"Fajl nije pronađen: {transformed_parquet}")

        table = pq.read_pandas(transformed_parquet).to_pandas()
        print("Podaci koji se unose u bazu:")
        print(table)
        
        rows_to_insert = [(row['statusId'], row['status_name']) for _, row in table.iterrows()]
        
        hook = PostgresHook(postgres_conn_id="formula_one_connection")

        hook.insert_rows(table="status", rows=rows_to_insert, target_fields=["statusid", "status_name"])
        
        return "Podaci uspešno uneti u bazu."
#endregion

#region CALLING_FUNCTIONS
    # Definicija toka podataka
    truncate_all_tables()
    extracted_files = extract_to_parquet()

    # STATUS
    transformed_file = transform_status_data(extracted_files["status"])
    load_status_data(transformed_file)

    # CONSTRUCTOR STANDINGS
    transformed_constandings_file = transform_constandings_data(extracted_files["constandings"])
    load_constandings_data(transformed_constandings_file)

    # DRIVER STANDINGS
    transformed_driverstandings_file = transform_driverstandings_data(extracted_files["driverstandings"])
    load_driverstandings_data(transformed_driverstandings_file)

    # DRIVER
    transformed_driver_file = transform_driver_data(extracted_files["driver"])
    load_driver_data(transformed_driver_file)

    # CONSTRUCTOR
    transformed_constructor_file = transform_constructor_data(extracted_files["constructor"])
    load_constructor_data(transformed_constructor_file)

    # PITSTOPS
    transformed_pitstops_file = transform_pitstops_data(extracted_files["pitstops"])
    load_pitstops_data(transformed_pitstops_file)

    # LAPTIMES
    transformed_laptimes_file = transform_laptimes_data(extracted_files["laptimes"])
    load_laptimes_data(transformed_laptimes_file)

    # QUALIFYING
    transformed_file = transform_qualifying_data(extracted_files["qualifying"])
    load_qualifying_data(transformed_file)

    # FREE PRACTICE
    transformed_freePractice_file = transform_freePractice_data(extracted_files["freePractice"])
    load_freePractice_data(transformed_freePractice_file)

    # SPRINT
    transformed_sprint_file = transform_sprint_data(extracted_files["sprint"])
    load_sprint_data(transformed_sprint_file)

    # CIRCUIT
    transformed_circuit_file = transform_circuit_data(extracted_files["circuit"])
    load_circuit_data(transformed_circuit_file)

    # RACE
    transformed_race_file = transform_race_data(extracted_files["race"])
    load_race_data(transformed_race_file)
    
    # FORMULA ONE
    transformed_formula_one_file = transform_formula_one_data(extracted_files["formula_one"])
    load_formula_one_data(transformed_formula_one_file)

#endregion


csv_to_postgres()
