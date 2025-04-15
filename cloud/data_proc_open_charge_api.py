import os
import subprocess
import logging
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, first, regexp_replace, current_date, year, to_date, udf, concat_ws,
    expr, array, lit, when
)
from pyspark.sql.types import StringType
from shapely.geometry import Point
import geopandas as gpd
from google.cloud import storage

# Konfigurationen
PROJECT_ID = '<your-project-id>'
BUCKET_NAME = '<your-bucket-name>'
DATASET_NAME = '<your-dataset-name>'
TABLE_NAME = '<your-table-name>'
DATA_PROC_CLUSTER = '<your-cluster-name>'
REGION = '<your-region>'
os.environ["PROJ_LIB"] = "/usr/share/proj"

logging.basicConfig(level=logging.INFO)

# Geo-Datenpfade
GEO_DATA_SOURCE_PATH = f"gs://{BUCKET_NAME}/data/shapes/"
GEO_DATA_DESTINATION_PATH = "/tmp/shapes"
os.makedirs(GEO_DATA_DESTINATION_PATH, exist_ok=True)
try:
    subprocess.run(["gsutil", "-m", "cp", "-r", GEO_DATA_SOURCE_PATH, GEO_DATA_DESTINATION_PATH], check=True)
except subprocess.CalledProcessError as e:
    logging.error("Error occurred during gsutil copy: %s", e)

def get_shape_file_information(shape_file_name: str) -> gpd.GeoDataFrame:
    path = os.path.join(GEO_DATA_DESTINATION_PATH, "shapes", shape_file_name)
    return gpd.read_file(path)

# SET UP SPARK
spark = (
    SparkSession.builder
    .appName("LadesaulenDataProcessingOpenChargeAPI")
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
    """Transformiert die Daten und entfernt Anomalien."""
    df = drop_unnecessary_columns(df)
    df = add_columns(df)
    df = rename_columns(df)
    df = parse_column_types(df)
    df = transform_connection_types(df)
    df = remove_special_characters(df)
    df = add_missing_values(df)
    anomalies_df = detect_anomalies(df)
    df = df.subtract(anomalies_df)
    return df, anomalies_df

def add_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("state", lit("N.A")).withColumn("district_city", lit("N.A"))

def drop_unnecessary_columns(df: DataFrame) -> DataFrame:
    df = df.na.drop(how="all")
    cols_to_drop = [
        'ID', 'UUID', ' LocationTitle', 'Distance', 'DistanceUnit', 'Addr_ContactTelephone1',
        'Addr_ContactTelephone2', 'Addr_ContactEmail', 'Addr_AccessComments', 'Addr_GeneralComments',
        'Addr_RelatedURL', 'ChargerType', 'GeneralComments', 'DateLastConfirmed', 'DateLastStatusUpdate',
        'Country', 'StateOrProvince'
    ]
    existing = [c for c in cols_to_drop if c in df.columns]
    return df.drop(*existing)

def standardize_connection_type(connection_type: str) -> str:
    mapping = {
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
    if not connection_type:
        return ""
    parts = [ct.strip() for ct in connection_type.split(';') if ct.strip()]
    standardized = []
    for ct in parts:
        std_type = mapping.get(ct, ct)
        if std_type not in standardized:
            standardized.append(std_type)
    return '; '.join(standardized)

standardize_connection_type_udf = udf(standardize_connection_type, StringType())

def transform_connection_types(df: DataFrame) -> DataFrame:
    return df.withColumn("plug_types", standardize_connection_type_udf(col("plug_types")))

def parse_column_types(df: DataFrame) -> DataFrame:
    df = df.withColumn('postal_code', col('postal_code').cast("int"))
    df = df.withColumn('address_additional', col('address_additional').cast("string"))
    df = df.withColumn('street', col('street').cast("string"))
    df = df.withColumn('longitude', regexp_replace(col('longitude'), ",", ".").cast("double"))
    df = df.withColumn('latitude', regexp_replace(col('latitude'), ",", ".").cast("double"))
    df = df.withColumn('number_of_charging_points', col('number_of_charging_points').cast("int"))
    df = df.withColumn("state", col("state").cast(StringType()))
    df = df.withColumn("district_city", col("district_city").cast(StringType()))
    return df

def rename_columns(df: DataFrame) -> DataFrame:
    mapping = {
        "AddressLine1": "street",
        "AddressLine2": "address_additional",
        "Town": "city",
        "Postcode": "postal_code",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "ConnectionType": "plug_types",
        "UsageType": "usage_type",
        "NumberOfPoints": "number_of_charging_points",
        "StatusType": "status_type",
        "DateCreated": "commissioning_date",
        "Operator": "operator"
    }
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df

def remove_special_characters(df: DataFrame) -> DataFrame:
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, regexp_replace(col(column), '\n', ''))
            df = df.withColumn(column, regexp_replace(col(column), r'\\+', ' '))
            df = df.withColumn(column, regexp_replace(col(column), r'\s+', ' '))
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
    if 'state' in df.columns and df.filter(col('state').isNull() | (col('state') == 'N.A')).count() > 0:
        df = df.withColumn(
            'state',
            when(
                col('state').isNull() | (col('state') == 'N.A') | (col('state') == 'UNKNOWN') | (col('state') == ''),
                get_state_from_coords_udf(col('longitude'), col('latitude'))
            ).otherwise(col('state'))
        )
    if 'district_city' in df.columns and df.filter(col('district_city').isNull() | (col('district_city') == 'N.A')).count() > 0:
        df = df.withColumn(
            'district_city',
            when(
                col('district_city').isNull() | (col('district_city') == 'N.A') | (col('district_city') == 'UNKNOWN') | (col('district_city') == ''),
                get_district_from_coords_udf(col('longitude'), col('latitude'))
            ).otherwise(col('district_city'))
        )
    if 'city' in df.columns:
        df = df.withColumn(
            'city',
            when(
                col('city').isNull() | (col('city') == 'N.A') | (col('city') == 'UNKNOWN') | (col('city') == ''),
                get_city_from_coords_udf(col('longitude'), col('latitude'))
            ).otherwise(col('city'))
        )
    if 'number_of_charging_points' in df.columns:
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
    logging.info("Latitude and longitude anomalies found: %d", anomalies.count())
    return anomalies

def detect_commissioning_date_anomalies(df: DataFrame) -> DataFrame:
    anomalies = df.filter(
        (year(to_date(col("commissioning_date"), "yyyy-MM-dd")).cast("int") < 2000) |
        (year(to_date(col("commissioning_date"), "yyyy-MM-dd")).cast("int") > year(current_date()))
    )
    logging.info("Commissioning date anomalies found: %d", anomalies.count())
    return anomalies

def detect_number_of_charging_points_anomalies(df: DataFrame) -> DataFrame:
    anomalies = df.filter(
        (col("number_of_charging_points") < 1) | (col("number_of_charging_points") > 20)
    )
    logging.info("Number of charging points anomalies found: %d", anomalies.count())
    return anomalies

def group_data(df: DataFrame) -> DataFrame:
    grouped = df.groupBy("latitude", "longitude").agg(
        first("street").alias("street"),
        first("address_additional").alias("address_additional"),
        first("postal_code").alias("postal_code"),
        first("city").alias("city"),
        first("district_city").alias("district_city"),
        first("state").alias("state"),
        first("commissioning_date").alias("commissioning_date"),
        first("plug_types").alias("plug_types"),
        first("usage_type").alias("usage_type"),
        first("status_type").alias("status_type"),
        first("number_of_charging_points").alias("number_of_charging_points")
    )
    return grouped

def merge_spark_fragments(directory_path: str, output_filename: str) -> DataFrame:
    df = spark.read.option("header", "true").csv(directory_path)
    if df.rdd.isEmpty():
        logging.info("No files found in %s", directory_path)
        return df
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_filename)
    logging.info("Merged files from %s into %s", directory_path, output_filename)
    return df

def save_transformed_data(df: DataFrame, output_path: str) -> None:
    try:
        df.write.mode("overwrite").option("header", "true").option("delimiter", ",").option("encoding", "UTF-8").csv(output_path)
        logging.info("Transformed data saved to %s", output_path)
    except Exception as e:
        logging.error("Error saving transformed data to %s: %s", output_path, e)

def main() -> None:
    raw_data_path = f"gs://{BUCKET_NAME}/open_charge_api/open_charge_ladesaulen.csv"
    transformed_data_path = f"gs://{BUCKET_NAME}/open_charge_api/transformed"
    grouped_data_path = f"gs://{BUCKET_NAME}/open_charge_api/grouped"
    anomalies_path = f"gs://{BUCKET_NAME}/open_charge_api/anomalies"
    merged_data_path = f"gs://{BUCKET_NAME}/open_charge_api/merged"

    raw_df = spark.read.option("mode", "PERMISSIVE") \
        .option("delimiter", ",") \
        .option("header", "true") \
        .option("encoding", "utf-8") \
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
