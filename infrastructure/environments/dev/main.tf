provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "Hirenovik"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

module "storage" {
  source      = "../../modules/storage"
  environment = var.environment
}
