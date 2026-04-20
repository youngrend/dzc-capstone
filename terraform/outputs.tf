output "gcs_bucket_url" {
  description = "GCS bucket URI"
  value       = "gs://${google_storage_bucket.raw.name}"
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}
