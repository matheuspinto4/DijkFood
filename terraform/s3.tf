resource "aws_s3_bucket" "grafo" {
    bucket = "dijkfood-grafo-sp-${data.aws_caller_identity.current.account_id}"
    force_destroy = true
}

resource "aws_s3_bucket_versioning" "grafo" {
    bucket = aws_s3_bucket.grafo.id

    versioning_configuration {
      status = "Enabled"
    }
}

resource "aws_s3_bucket" "datalake" {
  bucket        = "dijkfood-datalake-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}
