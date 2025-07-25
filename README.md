**Project Title:** Automated Word Count Pipeline on GCP using Cloud Composer, Dataproc & BigQuery

---

**Objective:**
Build a production-grade, fully automated data pipeline on Google Cloud Platform that:

* Runs a PySpark job on Dataproc to perform a word count on a text file stored in GCS
* Stores the result (CSV) in GCS
* Automatically loads the result into BigQuery
* All orchestrated via Cloud Composer (Apache Airflow)

---

## Step-by-Step Implementation

### 🔍 1. Project & Environment Setup

* **Create GCP Project**: (if new)

  ```bash
  gcloud projects create airflow-wordcount-project
  gcloud config set project airflow-wordcount-project
  ```

* **Enable Required APIs**:

  ```bash
  gcloud services enable \
      composer.googleapis.com \
      dataproc.googleapis.com \
      storage.googleapis.com \
      bigquery.googleapis.com \
      cloudfunctions.googleapis.com \
      iam.googleapis.com \
      cloudresourcemanager.googleapis.com \
      cloudbuild.googleapis.com
  ```

* **Create GCS Bucket**:

  ```bash
  gsutil mb -l us-central1 gs://airflow-wordcount-bucket
  ```

### 🚀 2. Create Dataproc Cluster

```bash
 gcloud dataproc clusters create demo-cluster \
   --region=us-central1 \
   --zone=us-central1-a \
   --single-node \
   --master-machine-type=n1-standard-2 \
   --image-version=2.1-debian11
```

### ⚙️ 3. Create Cloud Composer Environment

```bash
 gcloud composer environments create wordcount-composer \
   --location=us-central1 \
   --environment-size=ENVIRONMENT_SIZE_SMALL \
   --image-version=composer-2.3.4-airflow-2.6.3
```

> ⏳ This step takes 10-20 minutes.

---

## 📂 4. Prepare Your Files and Folders

* **wordcount.py** (PySpark Script)
* **readme.md** (Text file with sample data)
* **wordcount\_dag.py** (Airflow DAG)

**Upload to GCS**:

```bash
gsutil cp wordcount.py gs://airflow-wordcount-bucket/scripts/
gsutil cp readme.md gs://airflow-wordcount-bucket/input/
```

**Upload DAG to Composer DAG folder**:

* Navigate to Composer Environment > Configuration > DAGs Folder
* Upload `wordcount_dag.py` to that GCS path

---

## ⚡ 5. IAM Permissions

Service account used by Composer:

```
328838413973-compute@developer.gserviceaccount.com
```

Assign these roles:

```bash
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:328838413973-compute@developer.gserviceaccount.com" \
  --role="roles/composer.worker"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:328838413973-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.editor"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:328838413973-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:328838413973-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:328838413973-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.user"
```

---

## 🔢 6. BigQuery Setup

Create dataset (table will be auto-created by DAG):

```bash
bq mk --dataset --location=us wordcount_ds
```

---

## 🌐 7. Airflow DAG Logic

**DAG ID**: `wordcount_dataproc_to_bigquery`

### Tasks:

1. **`run_wordcount_job`**

   * Submits PySpark script to Dataproc
2. **`load_wordcount_to_bq`**

   * Uses `GCSToBigQueryOperator` to load result into BigQuery

Set dependency:

```python
run_dataproc_job >> load_to_bq
```

Trigger DAG from Airflow UI once all roles are granted and DAG is uploaded.

---

## 🎯 Final Result

You now have a fully functional GCP pipeline:

```
[GCS: readme.md]
   ↓
[Dataproc: wordcount.py]
   ↓
[GCS output: CSVs]
   ↓
[BigQuery: wordcount_ds.wordcount_result]
```

All triggered and orchestrated via **Cloud Composer (Airflow)**.

---

## ✅ Interview-Ready Highlights

* Dataproc orchestration using Airflow
* GCS to BigQuery ETL pipeline
* IAM roles management
* Cloud-native scheduling and retry handling

---

Let me know if you'd like me to export this to a downloadable `.pdf` or `.docx` file.
