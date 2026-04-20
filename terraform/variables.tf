variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "GCS bucket for raw Parquet files"
  type        = string
  default     = "ahrf-raw-dy"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset for raw layer"
  type        = string
  default     = "ahrf_raw"
}
