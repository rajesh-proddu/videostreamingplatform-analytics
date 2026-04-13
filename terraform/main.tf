terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "videostreamingplatform-terraform-state"
    key    = "analytics/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# Import shared infra outputs (VPC, EKS, etc.)
data "terraform_remote_state" "infra" {
  backend = "s3"
  config = {
    bucket = "videostreamingplatform-terraform-state"
    key    = "infra/terraform.tfstate"
    region = "us-east-1"
  }
}
