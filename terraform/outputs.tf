output "api_url" {
  description = "URL pública da API"
  value = "http://${aws_lb.api.dns_name}"
}

output "rds_endpoint" {
  description = "Endpoint do banco PostgreSQL"
  value = aws_db_instance.postgres.address
  sensitive = true
}

output "s3_bucket" {
  description = "Nome do bucket S3 do grafo viário"
  value = aws_s3_bucket.grafo.id
}