.PHONY: help init plan apply destroy run deploy dashboard

TF_DIR := terraform
BRUIN_DIR := citibike-pipeline

GCP_CREDENTIALS ?=
GCP_PROJECT ?=
FULL_REFRESH ?=
START_DATE ?=
END_DATE ?=

TF_VARS := \
	$(if $(GCP_CREDENTIALS),-var "credentials=$(GCP_CREDENTIALS)",) \
	$(if $(GCP_PROJECT),-var "project=$(GCP_PROJECT)",)

help:
	@echo ""
	@echo "Available commands:"
	@echo "  make init        - terraform init"
	@echo "  make plan        - terraform plan"
	@echo "  make apply       - terraform apply"
	@echo "  make destroy     - terraform destroy"
	@echo "  make deploy      - init + plan + apply"
	@echo "  make run         - run bruin pipeline"
	@echo "  make dashboard   - launch Dash dashboard"
	@echo ""
	@echo "Terraform options:"
	@echo "  GCP_CREDENTIALS=path/to/credentials.json"
	@echo "  GCP_PROJECT=my-gcp-project-id"
	@echo ""
	@echo "Run options:"
	@echo "  FULL_REFRESH=1"
	@echo "  START_DATE=YYYY-MM-DD"
	@echo "  END_DATE=YYYY-MM-DD"
	@echo ""

init:
	terraform -chdir=$(TF_DIR) init

plan:
	terraform -chdir=$(TF_DIR) plan $(TF_VARS)

apply:
	terraform -chdir=$(TF_DIR) apply $(TF_VARS)

destroy:
	terraform -chdir=$(TF_DIR) destroy $(TF_VARS)

deploy: init plan apply

run:
	bruin run ./$(BRUIN_DIR) \
	$(if $(FULL_REFRESH),--full-refresh,) \
	$(if $(START_DATE),--start-date $(START_DATE),) \
	$(if $(END_DATE),--end-date $(END_DATE),)

dashboard:
	cd dashboard && uv run plotly app run app:app --host 0.0.0.0 --debug