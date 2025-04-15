import os
import subprocess
import logging
from typing import Tuple

from shapely.geometry import Point
import geopandas as gpd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, first, regexp_replace, current_date, year, to_date, udf, trim,
    split, concat_ws, array, expr, lit, when
)
from pyspark.sql.types import StringType

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

# Shape-Dateien
GEO_DATA_SOURCE_PATH = f"gs://{BUCKET_NAME}/data/shapes/"
GEO_DATA_DESTINATION_PATH = "/tmp/shapes"
os.makedirs(GEO_DATA_DESTINATION_PATH, exist_ok=True)
try:
    subprocess.run(["gsutil", "-m", "cp", "-r", GEO_DATA_SOURCE_PATH, GEO_DATA_DESTINATION_PATH], check=True)
except subprocess.CalledProcessError as e:
    logging.error(f"Error occurred during gsutil copy: {e}")

def get_shape_file_information(shape_file_name: str) -> gpd.GeoDataFrame:
    shape_file_path = os.path.join(GEO_DATA_DESTINATION_PATH, "shapes", shape_file_name)
    return gpd.read_file(shape_file_path)

# SET UP SPARK
spark = (
    SparkSession.builder
    .appName("LadesaulenDataProcessingOpenDataAPI")
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
    Transformiert die Daten mit Spark und führt die Anomalieerkennung durch.
    """
    df = add_columns(df)
    df = drop_unnecessary_columns(df)
    df = parse_column_types(df)
    df = rename_columns(df)
    df = convert_plug_and_power_columns(df)
    df = remove_special_characters(df)
    df = add_missing_values(df)

    anomalies_df = detect_anomalies(df)
    df = df.subtract(anomalies_df)

    return df, anomalies_df

def drop_unnecessary_columns(df: DataFrame) -> DataFrame:
    return df.na.drop(how="all")

def add_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("state", lit("N.A"))

def parse_column_types(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("latitude", trim(split(col("koordinaten"), ",")[0]))
          .withColumn("longitude", trim(split(col("koordinaten"), ",")[1]))
          .drop("koordinaten")
          .withColumn("state", col("state").cast(StringType()))
    )
    return df

def remove_special_characters(df: DataFrame) -> DataFrame:
    for column in df.columns:
        # Überprüfen, ob der Datentyp "string" ist
        if df.schema[column].dataType.simpleString() == 'string':
            df = df.withColumn(column, regexp_replace(col(column), '\n', ''))
    return df

def rename_columns(df: DataFrame) -> DataFrame:
    column_mapping = {
        "betreiber": "operator",
        "art_der_ladeeinrichung": "charging_facility_type",
        "anzahl_ladepunkte": "number_of_charging_points",
        "nennleistung_ladeeinrichtung_kw": "power_connection_capacity",
        "steckertypen1": "plug_type_1",
        "steckertypen2": "plug_type_2",
        "steckertypen3": "plug_type_3",
        "steckertypen4": "plug_type_4",
        "p1_kw": "power_capacity_kw_1",
        "p2_kw": "power_capacity_kw_2",
        "p3_kw": "power_capacity_kw_3",
        "p4_kw": "power_capacity_kw_4",
        "kreis_kreisfreie_stadt": "district_city",
        "ort": "city",
        "postleitzahl": "postal_code",
        "strasse": "street",
        "hausnummer": "house_number",
        "adresszusatz": "address_additional",
        "inbetriebnahmedatum": "commissioning_date",
    }
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

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

    plug_columns = [f"plug_type_{i}" for i in range(1, 5)]
    power_columns = [f"power_capacity_kw_{i}" for i in range(1, 5)]
    existing_plug_columns = [c for c in plug_columns if c in df.columns]
    existing_power_columns = [c for c in power_columns if c in df.columns]

    df = df.withColumn("plug_types", array(*[col(c) for c in existing_plug_columns]))
    df = df.withColumn("power_capacities_kw", array(*[col(c) for c in existing_power_columns]))

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

    df = df.drop(*existing_plug_columns).drop(*existing_power_columns)
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
        col('number_of_charging_points').isNull() | (col('number_of_charging_points') == '') | (col('number_of_charging_points') == 'N.A')
    ).count() > 0:
        df = df.withColumn(
            'number_of_charging_points',
            when(
                col('number_of_charging_points').isNull() | (col('number_of_charging_points') == '') | (col('number_of_charging_points') == 'N.A'),
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
        first("state").alias("state"),
        first("district_city").alias("district_city"),
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
        return None
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_filename)
    logging.info(f"Merged files from {directory_path} into {output_filename}")
    return df

def save_transformed_data(df: DataFrame, output_path: str) -> None:
    try:
        df.write.mode("overwrite").option("header", "true").option("delimiter", ",").option("encoding", "UTF-8").csv(output_path)
        logging.info(f"Transformed data saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving transformed data to {output_path}: {e}")

def main():
    raw_data_path = f"gs://{BUCKET_NAME}/open_data_api/open_data_ladesaulen.csv"
    transformed_data_path = f"gs://{BUCKET_NAME}/open_data_api/transformed"
    grouped_data_path = f"gs://{BUCKET_NAME}/open_data_api/grouped"
    anomalies_path = f"gs://{BUCKET_NAME}/open_data_api/anomalies"
    merged_data_path = f"gs://{BUCKET_NAME}/open_data_api/merged"

    raw_df = spark.read.option("mode", "PERMISSIVE") \
        .option("delimiter", ";") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .option("quote", "\"") \
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
