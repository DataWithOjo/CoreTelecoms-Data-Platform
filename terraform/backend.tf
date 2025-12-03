terraform {
  backend "s3" {
    bucket         = "coretelecoms-tf-state-oluwakayode"
    key            = "dev/terraform.tfstate"
    region         = "eu-north-1"
    dynamodb_table = "coretelecoms-tf-lock"
    encrypt        = true
  }
}