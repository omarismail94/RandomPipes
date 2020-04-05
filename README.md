
##Upload Template to Google Storage
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=org.omar.<class_name> \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=<project-id> \
--stagingLocation=gs://<bucket-name>/staging \
--tempLocation=gs://<bucket-name>/temp \
--templateLocation=gs://<bucket-name>/templates/<template-name>.json \
--runner=DataflowRunner"

##Execute on Dataflow
###If Template Exists on Google Storage
gcloud dataflow jobs run <job-name> \
--gcs-location=gs://<bucket-name>/templates/wordkey.json \
--region=us-central1 \
--parameters inputFile=gs://<bucket-name>/wordkeys.txt,output=gs://<bucket-name>/results/output/


##To Compile and Run on Dataflow
mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=org.omar.KeyWords \
      -Dexec.args=" \
      --project=<project-id> \
      --stagingLocation=gs://<bucket-name>/staging  \
      --output=gs://<bucket-name>/output \
      --inputFile=gs://<bucket-name>/wordkeys.txt \
      --runner=DataflowRunner"
      
##To Run Locally
mvn clean compile exec:java -Dexec.mainClass=org.omar.KeyWords "-Dexec.args=--output=results/output/ --inputFile=wordkeys.txt" -f pom.xml