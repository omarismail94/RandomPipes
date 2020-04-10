
## Upload Template to Google Storage
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=org.omar.$CLASS_NAME \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_ID \
--stagingLocation=gs://$BUCKET_NAME/staging \
--tempLocation=gs://$BUCKET_NAME/temp \
--templateLocation=gs://$BUCKET_NAME/templates/$TEMPLATE_NAME.json \
--runner=DataflowRunner"

## Execute on Dataflow
### If Template Exists on Google Storage
gcloud dataflow jobs run $JOB_NAME \
--gcs-location=gs://$BUCKET_NAME/templates/$TEMPLATE_NAME.json \
--region=us-central1 \
--parameters <Parameters from MyOptions in each class>


### To Compile and Run on Dataflow
mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=org.omar.$CLASS_NAME \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$BUCKET_NAME/staging  \
      --tempLocation=gs://$BUCKET_NAME/temp \
      --runner=DataflowRunner \
      --<Parameters from MyOptions in each class>
      "
      
## To Run Locally
mvn clean compile exec:java -Dexec.mainClass=org.omar.$CLASS_NAME
