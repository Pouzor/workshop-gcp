Google Cloud Function to detect new files on Google Cloud Storage.
Each time a new file is created, the CLoud Function launches a Dataflow job that will ingest the data.


    To deploy the Cloud Function

gcloud beta functions deploy loadData \
  --stage-bucket workshop-functions \
  --trigger-bucket workshop-data   
