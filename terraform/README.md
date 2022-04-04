## Setup Terraform
Terraform is an open source tool which has been used for provisioning infrastructure resources. In this case, it was used to create GCP Infra by creating the following [Terraform files](./terraform):
- `main.tf`
- `variables.tf`
- `.tfstate`

The necessary configurations were made to ensure the successful execution and also set the resources as `google_storage_bucket, google_bigquery_dataset, google_bigquery_table` thereby creating the bucket from the project and also creating the BigQuery dataset and the bigquery tables for all movie data and the transform data from DBT.

#### Execution steps
1. `terraform init`:
    * Initializes & configures the backend, installs plugins/providers, & checks out an existing configuration from a version control
2. `terraform plan`:
    * Matches/previews local changes against a remote state, and proposes an Execution Plan.
3. `terraform apply`:
    * Asks for approval to the proposed plan, and applies changes to cloud.

#### Execution
```
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```
```
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

[Back to home page](./project/README.md)