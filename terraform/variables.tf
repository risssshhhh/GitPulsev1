variable "aws_region" {
  type        = string
  description = "AWS region to deploy resources in"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Name of the project"
  default     = "gitpulse"
}

variable "environment" {
  type        = string
  description = "Target deployment environment"
  default     = "dev"
}

variable "admin_username" {
  type        = string
  description = "Admin username for Redshift Serverless"
  default     = "awsadmin"
}

variable "admin_password" {
  type        = string
  description = "Admin password for Redshift Serverless"
  sensitive   = true
  default     = "SecureP@ssw0rd2026!"
}
