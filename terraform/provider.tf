terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "CoreTelecoms-Data-Platform"
      Environment = "Dev"
      Owner       = "DataEngineeringTeam"
      ManagedBy   = "Terraform"
    }
  }
}

provider "snowflake" {
  organization_name = var.snowflake_org
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = "ACCOUNTADMIN"
}
