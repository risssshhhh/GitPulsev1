# 1. VPC Networking Resources (Required for Redshift Serverless Workgroup)
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name        = "${var.project_name}-${var.environment}-vpc"
    Environment = var.environment
  }
}

resource "aws_subnet" "subnet_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"

  tags = {
    Name = "${var.project_name}-${var.environment}-subnet-a"
  }
}

resource "aws_subnet" "subnet_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "${var.aws_region}b"

  tags = {
    Name = "${var.project_name}-${var.environment}-subnet-b"
  }
}

resource "aws_subnet" "subnet_c" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "${var.aws_region}c"

  tags = {
    Name = "${var.project_name}-${var.environment}-subnet-c"
  }
}

resource "aws_security_group" "redshift_sg" {
  name        = "${var.project_name}-${var.environment}-redshift-sg"
  description = "Allow port 5439 inbound for Redshift"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "PostgreSQL/Redshift access"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"] # Restricted to VPC CIDR, modify as needed for external clients
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-redshift-sg"
  }
}

# 2. S3 Lakehouse Bucket
resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${var.project_name}-lakehouse-data-bucket-unique"
  force_destroy = true

  tags = {
    Name        = "${var.project_name}-lakehouse"
    Environment = var.environment
  }
}

# Enable versioning on S3
resource "aws_s3_bucket_versioning" "lakehouse_versioning" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Block all public S3 access
resource "aws_s3_bucket_public_access_block" "lakehouse_public_block" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 3. IAM Roles for Redshift Spectrum (External Table queries on S3)
resource "aws_iam_role" "redshift_spectrum_role" {
  name = "${var.project_name}-${var.environment}-redshift-spectrum-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

# Attach S3 Read Only Policy
resource "aws_iam_role_policy_attachment" "s3_read_only" {
  role       = aws_iam_role.redshift_spectrum_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# Attach Glue Console Access for External Schemas / Catalog metadata
resource "aws_iam_role_policy_attachment" "glue_access" {
  role       = aws_iam_role.redshift_spectrum_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

# 4. Redshift Serverless Provisioning
resource "aws_redshiftserverless_namespace" "namespace" {
  namespace_name      = "${var.project_name}-${var.environment}-namespace"
  db_name             = var.project_name
  admin_username      = var.admin_username
  admin_user_password = var.admin_password
  iam_roles           = [aws_iam_role.redshift_spectrum_role.arn]

  tags = {
    Environment = var.environment
  }
}

resource "aws_redshiftserverless_workgroup" "workgroup" {
  workgroup_name = "${var.project_name}-${var.environment}-workgroup"
  namespace_name = aws_redshiftserverless_namespace.namespace.namespace_name
  
  subnet_ids = [
    aws_subnet.subnet_a.id,
    aws_subnet.subnet_b.id,
    aws_subnet.subnet_c.id
  ]
  security_group_ids = [aws_security_group.redshift_sg.id]
  publicly_accessible = false

  tags = {
    Environment = var.environment
  }
}
