## About the project

This code is my part of a group project on cloud computing and big data. The group project was an app for evaluating and displaying charging stations in Germany. Because I didn't implement the rest of the project myself, I only uploaded my part. My task was to implement a data processing pipeline in the Google Cloud.

The pipeline utilizes the following technologies:

- **Cloud Storage**: Storage of all raw data (CSV/JSON) from the APIs.
→ A scalable object storage service used to store and archive structured and unstructured data.  
- **Cloud Composer / Airflow**: Automated workflows (DAGs) for data extraction and transformation (ETL).
→ A fully managed workflow orchestration service based on Apache Airflow, used to schedule and monitor complex data pipelines.
- **DataProc (Spark/Hadoop)**: Scalable data processing and aggregation of large data volumes.
→ A managed Spark and Hadoop service for processing large-scale datasets efficiently.  
- **BigQuery**: Storage of processed data for fast analysis and SQL-based queries.
→ A serverless, highly scalable data warehouse designed for fast SQL analytics on large datasets.  
- **Looker Studio (formerly Data Studio)**: Creation of interactive dashboards.
→ A business intelligence tool that allows users to visualize and share data insights through customizable dashboards and reports.
- **Python**: Used to implement DataProc and Cloud Composer scripts.
→ A versatile programming language widely used for scripting, data processing, and orchestration tasks in cloud environments.


## Key Learnings

- Gained first hands-on experience with cloud-based data pipelines and architectures.

- Learned how to orchestrate automated ETL workflows using Cloud Composer (Airflow).

- Understood how to use DataProc (Spark/Hadoop) for scalable processing of large datasets.

- Explored BigQuery for fast, SQL-based analysis of structured data.

- Built interactive dashboards with Looker Studio, improving data communication skills.

- Improved Python skills, especially in the context of scripting for cloud services.

- Learned the importance of modular, scalable, and reproducible data workflows.

- Developed a better understanding of how different cloud tools integrate to support end-to-end data solutions.