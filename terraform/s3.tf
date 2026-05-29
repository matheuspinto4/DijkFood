resource "aws_s3_bucket" "grafo" {
    bucket = "dijkfood-grafo-sp-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "grafo" {
    bucket = aws_s3_bucket.grafo.id

    versioning_configuration {
      status = "Enable"
    }
}



