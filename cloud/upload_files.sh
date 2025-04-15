echo "Start Uploading files to Google Cloud Storage"

echo "Uploading Cloud Composer Configuration DAGs"
gsutil cp cloud_composer.py gs://ccbd_project_bucket/dags/

echo "Uploading DataProc Configuration Files"
echo "Upload Data Proc Deutschland API"
gsutil cp data_proc_deutschland_api.py gs://ccbd_project_bucket/scripts/

echo "Upload Data Proc Open Charge API"
gsutil cp data_proc_open_charge_api.py gs://ccbd_project_bucket/scripts/

echo "Upload Data Proc Open Data API"
gsutil cp data_proc_open_data_api.py gs://ccbd_project_bucket/scripts/

echo "Upload Data Merge Script"
gsutil cp merge_data_source.py gs://ccbd_project_bucket/scripts/

echo "Finished"