variable "credentials" {
  description = "GCP Credentials"
  default     = "de-admin-credentials"
}


variable "project" {
  description = "GCP Project ID"
  default     = "de-zoomcamp-project-490007"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage Bucket Name"
  default     = "nyc_citibike_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "bq_dataset_names" {
  description = "BigQuery datasets names"
  type        = set(string)

  default = [
    "raw",
    "staging",
    "report_dev",
    "report_prod"
  ]
}