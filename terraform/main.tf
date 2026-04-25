terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "videostreamingplatform-terraform-state"
    key            = "analytics/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region
}

# Import shared platform outputs (VPC, EKS OIDC, etc.) from the core repo.
data "terraform_remote_state" "platform" {
  backend = "s3"
  config = {
    bucket = "videostreamingplatform-terraform-state"
    key    = "eks/terraform.tfstate"
    region = "us-east-1"
  }
}
