#!/usr/bin/env python3
import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Konfigurationen
PROJECT_ID = '<your-project-id>'
BUCKET_NAME = '<your-bucket-name>'
DATASET_NAME = '<your-dataset-name>'
TABLE_NAME = '<your-table-name>'
DATA_PROC_CLUSTER = '<your-cluster-name>'
REGION = '<your-region>'

# Definierte Spaltenreihenfolge
COLUMN_ORDER = [
    'address_additional', 'charging_facility_type', 'city', 'commissioning_date',
    'district_city', 'house_number', 'latitude', 'longitude', 'number_of_charging_points',
    'operator', 'plug_types', 'postal_code', 'power_connection_capacity',
    'power_capacities_kw', 'state', 'street'
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


def merge_data_by_lat_long(df: DataFrame) -> DataFrame:
    """
    Aggregiert Zeilen, die denselben Wert für Latitude und Longitude besitzen.
    Dabei werden verschiedene Aggregationsfunktionen angewendet, um die Daten
    aus den unterschiedlichen Quellen zusammenzuführen.
    """
    aggregation = {
        "address_additional": F.first("address_additional", ignorenulls=True),
        "charging_facility_type": F.first("charging_facility_type", ignorenulls=True),
        "city": F.first("city", ignorenulls=True),
        "commissioning_date": F.max("commissioning_date"),  # Neuestes Datum
        "district_city": F.first("district_city", ignorenulls=True),
        "house_number": F.first("house_number", ignorenulls=True),
        "number_of_charging_points": F.sum("number_of_charging_points"),
        "operator": F.first("operator", ignorenulls=True),
        "plug_types": F.array_distinct(F.collect_list("plug_types")),
        "power_capacities_kw": F.array_distinct(F.collect_list("power_capacities_kw")),
        "postal_code": F.first("postal_code", ignorenulls=True),
        "power_connection_capacity": F.max("power_connection_capacity"),
        "state": F.first("state", ignorenulls=True),
        "street": F.first("street", ignorenulls=True),
    }
    merged_df = df.groupBy("latitude", "longitude").agg(
        *[agg.alias(col_name) for col_name, agg in aggregation.items()]
    )
    # Listen in Strings umwandeln
    merged_df = merged_df.withColumn("plug_types", F.concat_ws(",", "plug_types"))
    merged_df = merged_df.withColumn("power_capacities_kw", F.concat_ws(",", "power_capacities_kw"))
    return merged_df


def merge_data_from_directories(input_paths: List[str], output_path: str) -> None:
    """
    Liest CSV-Dateien aus den angegebenen Input-Pfaden ein, ergänzt fehlende Spalten,
    vereinigt die DataFrames und führt sie anhand von Latitude und Longitude zusammen.
    Anschließend wird das Ergebnis im angegebenen Output-Pfad gespeichert.
    """
    spark = SparkSession.builder.appName("DataMergingApp").getOrCreate()
    dfs = []

    for path in input_paths:
        try:
            df = spark.read.options(header="true", delimiter=",", encoding="UTF-8").csv(path)
            for col_name in COLUMN_ORDER:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, F.lit(None))
            df = df.select(COLUMN_ORDER)
            dfs.append(df)
            logging.info("Successfully read data from %s", path)
        except Exception as e:
            logging.error("Error reading data from %s: %s", path, e)

    if dfs:
        try:
            merged_df = dfs[0]
            for additional_df in dfs[1:]:
                merged_df = merged_df.union(additional_df)
            merged_df = merge_data_by_lat_long(merged_df)
            # Auf eine Partition reduzieren, um doppelte Header zu vermeiden
            merged_df = merged_df.coalesce(1)
            merged_df.write.mode("overwrite").options(header="true", delimiter=",", encoding="UTF-8").csv(output_path)
            logging.info("Merged data saved to %s", output_path)
            logging.info("Total records: %d", merged_df.count())
            logging.info("Columns: %s", ", ".join(merged_df.columns))
        except Exception as e:
            logging.error("Merging error: %s", e)
    else:
        logging.info("No dataframes to merge.")

    spark.stop()


if __name__ == "__main__":
    # Für Produktion:
    input_paths = [
        f"gs://{BUCKET_NAME}/deutschland_api/merged/",
        f"gs://{BUCKET_NAME}/open_charge_api/merged/",
        f"gs://{BUCKET_NAME}/open_data_api/merged/"
    ]
    output_path = f"gs://{BUCKET_NAME}/bigquery/"
    merge_data_from_directories(input_paths, output_path)
