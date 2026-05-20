terraform {
  backend "s3" {
    bucket         = "hirenovik-tf-state-edgar-alfaro-2026"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "hirenovik-tf-locks"
    encrypt        = true
  }
}
