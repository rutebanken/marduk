# Contains main description of bulk of terraform?
terraform {
  required_version = ">= 0.12"
}

provider "google" {
  version = "~> 2.19"
}
provider "kubernetes" {
  load_config_file = var.load_config_file
}

# temporary adding back provider "random"
provider "random" {
  version = "~> 2.2.1"
}

# create service account
resource "google_service_account" "marduk_service_account" {
  account_id = "${var.labels.team}-${var.labels.app}-sa"
  display_name = "${var.labels.team}-${var.labels.app} service account"
  project = var.gcp_resources_project
}

# add service account as member to the cloudsql client
resource "google_project_iam_member" "cloudsql_iam_member" {
  project = var.gcp_cloudsql_project
  role = var.service_account_cloudsql_role
  member = "serviceAccount:${google_service_account.marduk_service_account.email}"
}

# add service account as member to the main bucket
resource "google_storage_bucket_iam_member" "storage_main_bucket_iam_member" {
  bucket = var.bucket_marduk_instance_name
  role = var.service_account_bucket_role
  member = "serviceAccount:${google_service_account.marduk_service_account.email}"
}

# add service account as member to the exchange bucket
resource "google_storage_bucket_iam_member" "storage_exchange_bucket_iam_member" {
  bucket = var.bucket_exchange_instance_name
  role = var.service_account_bucket_role
  member = "serviceAccount:${google_service_account.marduk_service_account.email}"
}

# add service account as member to the otpreport bucket
resource "google_storage_bucket_iam_member" "storage_otpreport_bucket_iam_member" {
  bucket = var.bucket_otpreport_instance_name
  role = var.service_account_bucket_role
  member = "serviceAccount:${google_service_account.marduk_service_account.email}"
}

# add service account as member to pubsub service in the resources project
resource "google_project_iam_member" "pubsub_project_iam_member" {
  project = var.gcp_pubsub_project
  role = var.service_account_pubsub_role
  member = "serviceAccount:${google_service_account.marduk_service_account.email}"
}

# create key for service account
resource "google_service_account_key" "marduk_service_account_key" {
  service_account_id = google_service_account.marduk_service_account.name
}

# Add SA key to to k8s
resource "kubernetes_secret" "marduk_service_account_credentials" {
  metadata {
    name = "${var.labels.team}-${var.labels.app}-sa-key"
    namespace = var.kube_namespace
  }
  data = {
    "credentials.json" = "${base64decode(google_service_account_key.marduk_service_account_key.private_key)}"
  }
}

resource "kubernetes_secret" "ror-marduk-secret" {
  metadata {
    name = "${var.labels.team}-${var.labels.app}-secret"
    namespace = var.kube_namespace
  }

  data = {
    "marduk-db-username" = var.ror-marduk-db-username
    "marduk-db-password" = var.ror-marduk-db-password
    "marduk-google-sftp-username" = var.ror-marduk-google-sftp-username
    "marduk-google-sftp-password" = var.ror-marduk-google-sftp-password
    "marduk-google-qa-sftp-username" = var.ror-marduk-google-qa-sftp-username
    "marduk-google-qa-sftp-password" = var.ror-marduk-google-qa-sftp-password
    "marduk-keycloak-secret" = var.ror-marduk-keycloak-secret
  }
}

/** Deactivate resource creation until workloads are moved to the new cluster.

module "postgres" {
  source = "github.com/entur/terraform//modules/postgres"
  postgresql_version = "POSTGRES_9_6"
  gcp_project = var.gcp_cloudsql_project
  labels = var.labels
  kubernetes_namespace = var.kube_namespace
  db_name = "marduk"
  db_user = "marduk"
  region = var.db_region
  zoneLetter = var.db_zone_letter
  db_instance_tier = "db-custom-1-3840"
  db_instance_disk_size = 10
  db_instance_backup_enabled = true
  availability_type = var.db_availability_type
}

# Create pubsub topics and subscriptions
resource "google_pubsub_topic" "ChouetteExportGtfsQueue" {
  name = "ChouetteExportGtfsQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouetteExportGtfsQueue" {
  name = "ChouetteExportGtfsQueue"
  topic = google_pubsub_topic.ChouetteExportGtfsQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ChouetteExportNetexQueue" {
  name = "ChouetteExportNetexQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouetteExportNetexQueue" {
  name = "ChouetteExportNetexQueue"
  topic = google_pubsub_topic.ChouetteExportNetexQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ChouetteImportQueue" {
  name = "ChouetteImportQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouetteImportQueue" {
  name = "ChouetteImportQueue"
  topic = google_pubsub_topic.ChouetteImportQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ChouetteMergeWithFlexibleLinesQueue" {
  name = "ChouetteMergeWithFlexibleLinesQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouetteMergeWithFlexibleLinesQueue" {
  name = "ChouetteMergeWithFlexibleLinesQueue"
  topic = google_pubsub_topic.ChouetteMergeWithFlexibleLinesQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ChouettePollStatusQueue" {
  name = "ChouettePollStatusQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouettePollStatusQueue" {
  name = "ChouettePollStatusQueue"
  topic = google_pubsub_topic.ChouettePollStatusQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ChouetteTransferExportQueue" {
  name = "ChouetteTransferExportQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouetteTransferExportQueue" {
  name = "ChouetteTransferExportQueue"
  topic = google_pubsub_topic.ChouetteTransferExportQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ChouetteValidationQueue" {
  name = "ChouetteValidationQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ChouetteValidationQueue" {
  name = "ChouetteValidationQueue"
  topic = google_pubsub_topic.ChouetteValidationQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "GtfsBasicExportMergedQueue" {
  name = "GtfsBasicExportMergedQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "GtfsBasicExportMergedQueue" {
  name = "GtfsBasicExportMergedQueue"
  topic = google_pubsub_topic.GtfsBasicExportMergedQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "GtfsExportMergedQueue" {
  name = "GtfsExportMergedQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "GtfsExportMergedQueue" {
  name = "GtfsExportMergedQueue"
  topic = google_pubsub_topic.GtfsExportMergedQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "GtfsGoogleExportQueue" {
  name = "GtfsGoogleExportQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "GtfsGoogleExportQueue" {
  name = "GtfsGoogleExportQueue"
  topic = google_pubsub_topic.GtfsGoogleExportQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "GtfsGooglePublishQaQueue" {
  name = "GtfsGooglePublishQaQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "GtfsGooglePublishQaQueue" {
  name = "GtfsGooglePublishQaQueue"
  topic = google_pubsub_topic.GtfsGooglePublishQaQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "GtfsGooglePublishQueue" {
  name = "GtfsGooglePublishQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "GtfsGooglePublishQueue" {
  name = "GtfsGooglePublishQueue"
  topic = google_pubsub_topic.GtfsGooglePublishQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "GtfsGoogleQaExportQueue" {
  name = "GtfsGoogleQaExportQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "GtfsGoogleQaExportQueue" {
  name = "GtfsGoogleQaExportQueue"
  topic = google_pubsub_topic.GtfsGoogleQaExportQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "MardukInboundQueue" {
  name = "MardukInboundQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "MardukInboundQueue" {
  name = "MardukInboundQueue"
  topic = google_pubsub_topic.MardukInboundQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "NetexExportMergedQueue" {
  name = "NetexExportMergedQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "NetexExportMergedQueue" {
  name = "NetexExportMergedQueue"
  topic = google_pubsub_topic.NetexExportMergedQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "ProcessFileQueue" {
  name = "ProcessFileQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "ProcessFileQueue" {
  name = "ProcessFileQueue"
  topic = google_pubsub_topic.ProcessFileQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "OtpBaseGraphBuildQueue" {
  name = "OtpBaseGraphBuildQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "OtpBaseGraphBuildQueue" {
  name = "OtpBaseGraphBuildQueue"
  topic = google_pubsub_topic.OtpBaseGraphBuildQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "Otp2BaseGraphBuildQueue" {
  name = "Otp2BaseGraphBuildQueue"
  topic = google_pubsub_topic.OtpBaseGraphBuildQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_topic" "OtpGraphBuildQueue" {
  name = "OtpGraphBuildQueue"
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "OtpGraphBuildQueue" {
  name = "OtpGraphBuildQueue"
  topic = google_pubsub_topic.OtpGraphBuildQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}

resource "google_pubsub_subscription" "Otp2GraphBuildQueue" {
  name = "Otp2GraphBuildQueue"
  topic = google_pubsub_topic.OtpGraphBuildQueue.name
  project = var.gcp_pubsub_project
  labels = var.labels
}
**/