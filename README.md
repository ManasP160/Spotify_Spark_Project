# **Spotify ETL Pipeline using AWS Glue and Snowflake**

## **Introduction**
This project demonstrates an ETL (Extract, Transform, Load) pipeline that leverages the Spotify API to extract music data and process it using AWS Glue, incorporating Spark for transformations. The transformed data is then loaded into Snowflake using Snowpipe for automated database creation. This data is further utilized to build dashboards, providing insights into the top songs globally.

## **Architecture Overview**
The architecture consists of three main stages: Extract, Transform, and Load (ETL), with the following workflow:

1. **Extract:** 
   - Data is extracted from the Spotify API.
2. **Transform:** 
   - The transformation is handled by AWS Glue using Spark, enabling efficient data processing.
3. **Load:** 
   - The transformed data is loaded into Snowflake using Snowpipe, which automatically creates the database schema and tables.
   - Once the data is in Snowflake, it can be used to create interactive dashboards in BI tools like Power BI.

![Architecture Diagram](https://github.com/ManasP160/Spotify_Spark_Project/blob/main/Spotify_spark_architecture_diagram.png)

## **Dataset/API Details**
The dataset is sourced from the Spotify API, focusing on the Top Songs Global Playlist. It contains detailed metadata such as song titles, artists, albums, and more. The API provides dynamic data that updates regularly, reflecting the latest music trends.
[Spotify API Documentation](https://developer.spotify.com/documentation/web-api)

### **Spotify API Documentation**
- The Spotify API provides comprehensive information about tracks, playlists, albums, and artists, facilitating efficient data extraction.

## **AWS Services Used**
- **Amazon S3 (Simple Storage Service):** 
  - Used for storing raw and transformed data.
- **AWS Glue:** 
  - AWS Glue handles data transformation using Spark sessions. The transformation logic is written in PySpark, enabling seamless processing and conversion of raw data into structured formats.
- **Amazon CloudWatch:** 
  - Monitors scheduled triggers and logs execution details for both the extraction and transformation processes.
- **Snowflake with Snowpipe:** 
  - Snowpipe is used to automate the ingestion of transformed data from S3 to Snowflake, dynamically creating databases, schemas, and tables.
- **Amazon Athena:** 
  - Optional service for querying the raw data in S3 to verify the transformation results before loading them into Snowflake.
- **Power BI:** 
  - Used to create interactive dashboards and analyze data stored in Snowflake.

## **Project Execution Flow**

### **1. Extract Data from API**
   - Data is extracted from the Spotify API using AWS Lambda, which is triggered based on a schedule set by Amazon CloudWatch.

### **2. Store Raw Data in S3**
   - The raw data from the API is stored in an Amazon S3 bucket for further processing.

### **3. Transform Data using AWS Glue**
   - AWS Glue, configured to use a Spark session, processes the raw data.
   - The transformation involves cleaning, filtering, and restructuring the data, preparing it for loading into Snowflake.
   - The transformed data is then stored in a separate S3 bucket.

### **4. Load Data into Snowflake using Snowpipe**
   - Snowpipe automatically ingests the transformed data from S3 into Snowflake.
   - The Snowpipe process creates the database and tables dynamically, mapping the incoming data to the appropriate structure.
   - Once ingested, the data is ready for analysis in Snowflake.

### **5. Build Dashboards using Power BI**
   - Data in Snowflake is visualized using Power BI, enabling users to create interactive dashboards that provide insights into global music trends.

## **Packages Installed**
- `pandas`
- `spotify`
- `numpy`
- `boto3` (for AWS interactions)
- `pyspark` (for data transformation in AWS Glue)

## **Conclusion**
This project effectively demonstrates the use of AWS Glue with Spark for data transformation, followed by automated data loading into Snowflake using Snowpipe. By integrating these services, the pipeline ensures efficient and scalable data processing, which can be leveraged for building real-time dashboards in Power BI.
