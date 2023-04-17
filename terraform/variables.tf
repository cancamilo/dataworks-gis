locals {
  data_lake_bucket = "nasa_power_datalake"
}

variable "project" {
  description = "google cloud project name"
  type = string
  default = "dataworks-gis"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-southwest1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET_RAW" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "geos_flux_data"
}

variable "BQ_DATASET_DEV" {
  description = "BigQuery Dataset where dbt development models will be written"
  type        = string
  default     = "development"
}

variable "BQ_DATASET_PROD" {
  description = "BigQuery Dataset where dbt development models will be written"
  type        = string
  default     = "production"
}
