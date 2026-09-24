output "s3_bucket_name" {
  value       = aws_s3_bucket.lakehouse.id
  description = "The name of the S3 bucket created for the lakehouse data"
}

output "redshift_endpoint" {
  value       = aws_redshiftserverless_workgroup.workgroup.endpoint
  description = "The endpoint address for connection to the Redshift Serverless Workgroup"
}

output "redshift_database_name" {
  value       = aws_redshiftserverless_namespace.namespace.db_name
  description = "The name of the initial database created inside Redshift Serverless"
}

output "redshift_spectrum_iam_role_arn" {
  value       = aws_iam_role.redshift_spectrum_role.arn
  description = "The ARN of the IAM role for Redshift Spectrum S3 query integration"
}
