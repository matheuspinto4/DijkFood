# ─────────────────────────────────────────────────────────────────────────────
# 1. PUXANDO A LABROLE (Usando a permissão existente do seu ambiente)
# ─────────────────────────────────────────────────────────────────────────────
# Em vez de criar um recurso (resource), usamos um 'data' para buscar o que já existe
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. CONFIGURAÇÃO DO DELIVERY STREAM (O Kinesis Firehose)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_kinesis_firehose_delivery_stream" "pedidos_firehose" {
  name        = "dijkfood-pedidos-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.order_events.arn
    # Usando o ARN da LabRole puxada lá em cima
    role_arn           = data.aws_iam_role.lab_role.arn
  }

  extended_s3_configuration {
    # Usando o ARN da LabRole puxada lá em cima
    role_arn   = data.aws_iam_role.lab_role.arn
    bucket_arn = aws_s3_bucket.datalake.arn

    prefix              = "pedidos/ano=!{timestamp:yyyy}/mes=!{timestamp:MM}/dia=!{timestamp:dd}/"
    error_output_prefix = "erros/ano=!{timestamp:yyyy}/mes=!{timestamp:MM}/dia=!{timestamp:dd}/!{firehose:error-output-type}/"

    buffering_size     = 5  
    buffering_interval = 60 
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
output "datalake_bucket_name" {
  value       = aws_s3_bucket.datalake.bucket
  description = "Copie este nome para usar no script do Redshift (setup_redshift.py)"
}