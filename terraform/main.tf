terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# GCS Bucket — raw Parquet landing zone
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "raw" {
  name          = var.gcs_bucket_name
  location      = "US-WEST1"   # hardcoded — bucket already exists here
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# ---------------------------------------------------------------------------
# BigQuery Dataset — raw layer
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.bq_dataset_id
  location                   = "us-central1"   # hardcoded — dataset already exists here
  delete_contents_on_destroy = false
}
