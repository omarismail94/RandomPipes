
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
--gcs-location=gs://$BUCKET_NAME/templates/wordkey.json \
--region=us-central1 \
--parameters inputFile=gs://$BUCKET_NAME/wordkeys.txt,output=gs://$BUCKET_NAME/results/output


### To Compile and Run on Dataflow
mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=org.omar.$CLASS_NAME \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$BUCKET_NAME/staging  \
      --output=gs://$BUCKET_NAME/results/output \
      --inputFile=gs://$BUCKET_NAME/wordkeys.txt \
      --runner=DataflowRunner"
      
## To Run Locally
mvn clean compile exec:java -Dexec.mainClass=org.omar.$CLASS_NAME -Dexec.args="--output=results/output/ --inputFile=wordkeys.txt" -f pom.xml
