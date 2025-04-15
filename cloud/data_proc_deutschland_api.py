import os
import subprocess
import logging
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, first, regexp_replace, current_date, year, to_date, udf, array, expr,
    concat_ws, when, lit
)
from pyspark.sql.types import StringType
from shapely.geometry import Point
import geopandas as gpd

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)

# Configs
PROJECT_ID = '<your-project-id>'
BUCKET_NAME = '<your-bucket-name>'
DATASET_NAME = '<your-dataset-name>'
TABLE_NAME = '<your-table-name>'
DATA_PROC_CLUSTER = '<your-cluster-name>'
REGION = '<your-region>'
os.environ["PROJ_LIB"] = "/usr/share/proj"

# Geo-Datenpfade
GEO_DATA_SOURCE_PATH = f"gs://{BUCKET_NAME}/data/shapes/"
GEO_DATA_DESTINATION_PATH = "/tmp/shapes"
os.makedirs(GEO_DATA_DESTINATION_PATH, exist_ok=True)
try:
    subprocess.run(
        ["gsutil", "-m", "cp", "-r", GEO_DATA_SOURCE_PATH, GEO_DATA_DESTINATION_PATH],
        check=True
    )
except subprocess.CalledProcessError as e:
    logging.error(f"Error occurred during gsutil copy: {e}")


def get_shape_file_information(shape_file_name: str) -> gpd.GeoDataFrame:
    shape_file_path = os.path.join(GEO_DATA_DESTINATION_PATH, "shapes", shape_file_name)
    return gpd.read_file(shape_file_path)


# SET UP SPARK
spark = (
    SparkSession.builder
    .appName("LadesaulenDataProcessingDeutschlandAPI")
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    .getOrCreate()
)
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Shapefiles laden und broadcasten
state_gdf = get_shape_file_information("gadm36_DEU_1.shp")
kreise_gdf = get_shape_file_information("gadm36_DEU_2.shp")
city_gdf = get_shape_file_information("gadm36_DEU_3.shp")

state_df_broadcast = spark.sparkContext.broadcast(state_gdf.to_dict("records"))
kreise_df_broadcast = spark.sparkContext.broadcast(kreise_gdf.to_dict("records"))
city_df_broadcast = spark.sparkContext.broadcast(city_gdf.to_dict("records"))


def transform_data(df: DataFrame):
    """
    Transformiert die Daten mithilfe von Spark und führt die Anomalieerkennung durch.
    """
    df = drop_unnecessary_columns(df)
    df = rename_columns(df)
    df = parse_column_types(df)
    df = convert_plug_and_power_columns(df)
    df = remove_special_characters(df)
    df = add_missing_values(df)
    anomalies_df = detect_anomalies(df)
    df = df.subtract(anomalies_df)
    return df, anomalies_df


def convert_plug_and_power_columns(df: DataFrame) -> DataFrame:
    PLUG_MAPPING = {
        "AC Typ 2 Steckdose": "Type 2 (AC)",
        "Type 2 (Socket Only)": "Type 2 (AC)",
        "Type 2 (Tethered Connector)": "Type 2 (AC)",
        "AC Typ 2 Fahrzeugkupplung": "Type 2 (AC)",
        "DC Fahrzeugkupplung Typ Combo 2 (CCS)": "CCS Combo 2 (DC)",
        "CCS (Type 2)": "CCS Combo 2 (DC)",
        "DC CHAdeMO": "CHAdeMO (DC)",
        "CHAdeMO": "CHAdeMO (DC)",
        "Tesla (Model S/X)": "Tesla Proprietary",
        "NACS / Tesla Supercharger": "Tesla Proprietary",
        "Tesla (Roadster)": "Tesla Proprietary",
        "Type 1 (J1772)": "Type 1 (AC)",
        "Type I (AS 3112)": "Type 1 (AC)"
    }

    # Für diesen Datensatz werden sechs Plug- und Power-Spalten erwartet
    plug_columns = [f"plug_type_{i}" for i in range(1, 7)]
    power_columns = [f"power_capacity_kw_{i}" for i in range(1, 7)]
    existing_plug_cols = [c for c in plug_columns if c in df.columns]
    existing_power_cols = [c for c in power_columns if c in df.columns]

    df = df.withColumn("plug_types", array(*[col(c) for c in existing_plug_cols]))
    df = df.withColumn("power_capacities_kw", array(*[col(c) for c in existing_power_cols]))

    for original, mapped in PLUG_MAPPING.items():
        df = df.withColumn(
            "plug_types",
            expr(f"transform(plug_types, x -> CASE WHEN x = '{original}' THEN '{mapped}' ELSE x END)")
        )

    df = df.withColumn("plug_types", concat_ws(",", col("plug_types")))
    df = df.withColumn(
        "power_capacities_kw",
        concat_ws(",", expr("filter(power_capacities_kw, x -> x > 1 AND x < 1000)"))
    )

    # Entferne die ursprünglichen Spalten
    df = df.drop(*existing_plug_cols).drop(*existing_power_cols)
    return df


def drop_unnecessary_columns(df: DataFrame) -> DataFrame:
    # Entferne komplett leere Zeilen sowie explizit unerwünschte Spalten
    df = df.na.drop(how="all")
    cols_to_drop = [
        'Anzeigename (Karte)', 'Public Key1', 'Public Key2', 'Public Key3',
        'Public Key4', 'Public Key5', 'Public Key6'
    ]
    # Nur existierende Spalten droppen
    cols_to_drop = [col_name for col_name in cols_to_drop if col_name in df.columns]
    return df.drop(*cols_to_drop)


def remove_special_characters(df: DataFrame) -> DataFrame:
    for column in df.columns:
        if df.schema[column].dataType.simpleString() == 'string':
            df = df.withColumn(column, regexp_replace(col(column), '\n', ' '))
            df = df.withColumn(column, regexp_replace(col(column), r'\\+', ' '))
            df = df.withColumn(column, regexp_replace(col(column), r'\s+', ' '))
    return df


def parse_column_types(df: DataFrame) -> DataFrame:
    df = df.withColumn('postal_code', col('postal_code').cast("int"))
    df = df.withColumn('address_additional', col('address_additional').cast("string"))
    df = df.withColumn('street', col('street').cast("string"))
    df = df.withColumn('operator', col('operator').cast("string"))
    df = df.withColumn('longitude', regexp_replace(col('longitude'), ",", ".").cast("double"))
    df = df.withColumn('latitude', regexp_replace(col('latitude'), ",", ".").cast("double"))
    return df


def rename_columns(df: DataFrame) -> DataFrame:
    column_mapping = {
        'Betreiber': 'operator',
        'Art der Ladeeinrichung': 'charging_facility_type',
        'Anzahl Ladepunkte': 'number_of_charging_points',
        'Nennleistung Ladeeinrichtung [kW]': 'power_connection_capacity',
        'Steckertypen1': 'plug_type_1',
        'Steckertypen2': 'plug_type_2',
        'Steckertypen3': 'plug_type_3',
        'Steckertypen4': 'plug_type_4',
        'Steckertypen5': 'plug_type_5',
        'Steckertypen6': 'plug_type_6',
        'P1 [kW]': 'power_capacity_kw_1',
        'P2 [kW]': 'power_capacity_kw_2',
        'P3 [kW]': 'power_capacity_kw_3',
        'P4 [kW]': 'power_capacity_kw_4',
        'P5 [kW]': 'power_capacity_kw_5',
        'P6 [kW]': 'power_capacity_kw_6',
        'Kreis/kreisfreie Stadt': 'district_city',
        'Ort': 'city',
        'Postleitzahl': 'postal_code',
        'Straße': 'street',
        'Hausnummer': 'house_number',
        'Adresszusatz': 'address_additional',
        'Bundesland': 'state',
        'Inbetriebnahmedatum': 'commissioning_date',
        'Breitengrad': 'latitude',
        'Längengrad': 'longitude'
    }
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def get_state_from_coords_udf(longitude, latitude):
    state_gdf = gpd.GeoDataFrame.from_records(state_df_broadcast.value)
    try:
        point = Point(float(longitude), float(latitude))
        matching_states = state_gdf[state_gdf.geometry.contains(point)]
        return matching_states['NAME_1'].iloc[0] if len(matching_states) > 0 else "UNKNOWN"
    except:
        return "UNKNOWN"


def get_district_from_coords_udf(longitude, latitude):
    kreise_gdf = kreise_df_broadcast.value
    try:
        point = Point(float(longitude), float(latitude))
        for idx, row in kreise_gdf.iterrows():
            if row['geometry'].contains(point):
                return row['NAME_2']
        return "UNKNOWN"
    except:
        return "UNKNOWN"


def get_city_from_coords_udf(longitude, latitude):
    city_gdf = city_df_broadcast.value
    try:
        point = Point(float(longitude), float(latitude))
        for idx, row in city_gdf.iterrows():
            if row['geometry'].contains(point):
                return row['NAME_3']
        return "UNKOWN"
    except:
        return "UNKOWN"


get_state_from_coords_udf = udf(get_state_from_coords_udf, StringType())
get_district_from_coords_udf = udf(get_district_from_coords_udf, StringType())
get_city_from_coords_udf = udf(get_city_from_coords_udf, StringType())


def add_missing_values(df: DataFrame) -> DataFrame:
    if 'state' in df.columns and df.filter(
        col('state').isNull() | (col('state') == 'N.A') | (col('state') == 'UNKNOWN') | (col('state') == '')
    ).count() > 0:
        df = df.withColumn(
            'state',
            when(
                col('state').isNull() | (col('state') == 'N.A') | (col('state') == 'UNKNOWN') | (col('state') == ''),
                get_state_from_coords_udf(col('longitude'), col('latitude'))
            ).otherwise(col('state'))
        )

    if 'district_city' in df.columns and df.filter(
        col('district_city').isNull() | (col('district_city') == 'N.A') | (col('district_city') == 'UNKNOWN') | (col('district_city') == '')
    ).count() > 0:
        df = df.withColumn(
            'district_city',
            when(
                col('district_city').isNull() | (col('district_city') == 'N.A') | (col('district_city') == 'UNKNOWN') | (col('district_city') == ''),
                get_district_from_coords_udf(col('longitude'), col('latitude'))
            ).otherwise(col('district_city'))
        )

    if 'city' in df.columns and df.filter(
        col('city').isNull() | (col('city') == 'N.A') | (col('city') == 'UNKNOWN') | (col('city') == '')
    ).count() > 0:
        df = df.withColumn(
            'city',
            when(
                col('city').isNull() | (col('city') == 'N.A') | (col('city') == 'UNKNOWN') | (col('city') == ''),
                get_city_from_coords_udf(col('longitude'), col('latitude'))
            ).otherwise(col('city'))
        )

    if 'number_of_charging_points' in df.columns and df.filter(
        col('number_of_charging_points').isNull() |
        (col('number_of_charging_points') == '') |
        (col('number_of_charging_points') == 'N.A')
    ).count() > 0:
        df = df.withColumn(
            'number_of_charging_points',
            when(
                col('number_of_charging_points').isNull() |
                (col('number_of_charging_points') == '') |
                (col('number_of_charging_points') == 'N.A'),
                lit(1)
            ).otherwise(col('number_of_charging_points'))
        )

    return df


def detect_anomalies(df: DataFrame) -> DataFrame:
    anomalies_lat_long = detect_lat_long_anomalies(df)
    anomalies_commissioning = detect_commissioning_date_anomalies(df)
    anomalies_charging = detect_number_of_charging_points_anomalies(df)
    return anomalies_lat_long.union(anomalies_commissioning).union(anomalies_charging)


def detect_lat_long_anomalies(df: DataFrame) -> DataFrame:
    min_lat, max_lat = 47.2, 55.1
    min_lon, max_lon = 5.5, 15.1
    anomalies = df.filter(
        (col("latitude") < min_lat) | (col("latitude") > max_lat) |
        (col("longitude") < min_lon) | (col("longitude") > max_lon)
    )
    logging.info(f"Latitude and longitude anomalies found: {anomalies.count()}")
    return anomalies


def detect_commissioning_date_anomalies(df: DataFrame) -> DataFrame:
    anomalies = df.filter(
        (year(to_date(col("commissioning_date"), "yyyy-MM-dd")).cast("int") < 2000) |
        (year(to_date(col("commissioning_date"), "yyyy-MM-dd")).cast("int") > year(current_date()))
    )
    logging.info(f"Commissioning date anomalies found: {anomalies.count()}")
    return anomalies


def detect_number_of_charging_points_anomalies(df: DataFrame) -> DataFrame:
    anomalies = df.filter(
        (col("number_of_charging_points") < 1) | (col("number_of_charging_points") > 20)
    )
    logging.info(f"Number of charging points anomalies found: {anomalies.count()}")
    return anomalies


def group_data(df: DataFrame) -> DataFrame:
    grouped_data = df.groupBy("latitude", "longitude").agg(
        first("operator").alias("operator"),
        first("street").alias("street"),
        first("house_number").alias("house_number"),
        first("address_additional").alias("address_additional"),
        first("postal_code").alias("postal_code"),
        first("city").alias("city"),
        first("district_city").alias("district_city"),
        first("state").alias("state"),
        first("commissioning_date").alias("commissioning_date"),
        first("power_connection_capacity").alias("power_connection_capacity"),
        first("charging_facility_type").alias("charging_facility_type"),
        first("number_of_charging_points").alias("number_of_charging_points"),
        first("plug_types").alias("plug_types"),
        first("power_capacities_kw").alias("power_capacities_kw")
    )
    return grouped_data


def merge_spark_fragments(directory_path: str, output_filename: str) -> DataFrame:
    df = spark.read.option("header", "true").csv(directory_path)
    if df.rdd.isEmpty():
        logging.info(f"No files found in {directory_path}")
        return df
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_filename)
    logging.info(f"Merged files from {directory_path} into {output_filename}")
    return df


def save_transformed_data(df: DataFrame, output_path: str) -> None:
    try:
        df.write.mode("overwrite").option("header", "true").option("delimiter", ",").option("encoding", "UTF-8").csv(output_path)
        logging.info(f"Transformed data saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving transformed data to {output_path}: {e}")


def main() -> None:
    raw_data_path = f"gs://{BUCKET_NAME}/deutschland_api/deutschland_api_ladesaulen.csv"
    transformed_data_path = f"gs://{BUCKET_NAME}/deutschland_api/transformed"
    grouped_data_path = f"gs://{BUCKET_NAME}/deutschland_api/grouped"
    anomalies_path = f"gs://{BUCKET_NAME}/deutschland_api/anomalies"
    merged_data_path = f"gs://{BUCKET_NAME}/deutschland_api/merged"

    raw_df = spark.read.option("mode", "PERMISSIVE") \
        .option("delimiter", ";") \
        .option("header", "true") \
        .option("encoding", "utf-8") \
        .option("escape", "\\") \
        .csv(raw_data_path)

    transformed_df, anomalies_df = transform_data(raw_df)
    grouped_df = group_data(transformed_df)

    save_transformed_data(anomalies_df, anomalies_path)
    save_transformed_data(transformed_df, transformed_data_path)
    save_transformed_data(grouped_df, grouped_data_path)

    merge_spark_fragments(grouped_data_path, merged_data_path)
    logging.info("Data processing completed successfully")


if __name__ == "__main__":
    main()
