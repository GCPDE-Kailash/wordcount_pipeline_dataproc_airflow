Dataproc commands for creating a cluster-


gcloud dataproc clusters create demo-cluster \
  --region=us-central1 \
  --zone=us-central1-a \
  --single-node \
  --master-machine-type=n1-standard-2 \
  --image-version=2.0-debian10 \
  --project=<your-project-id>
