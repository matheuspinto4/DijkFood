# Fila para mensagens que falharam (Dead Letter Queue)
resource "aws_sqs_queue" "dijkfood_dlq" {
  name = "dijkfood-orders-dlq"
  message_retention_seconds = 1209600 # 14 dias
}

# Fila principal de pedidos 
resource "aws_sqs_queue" "dijkfood_orders_queue" {
  name = "dijkfood-orders-queue"
  delay_seconds = 0
  max_message_size = 262144
  message_retention_seconds = 345600 # 4 dias
  receive_wait_time_seconds = 10 # Long Polling 

  # Conecta a fila principal à DLQ
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dijkfood_dlq.arn
    maxReceiveCount = 3 
  })

  tags = {
    Environment = "dev"
    Project = "DijkFood"
  }
}

# Outputs para referenciar em outros arquivos
output "sqs_queue_url" {
  description = "A URL da fila SQS para ser injetada nos containers ECS"
  value       = aws_sqs_queue.dijkfood_orders_queue.url
}

output "sqs_queue_arn" {
  description = "O ARN da fila SQS para configurar permissões no IAM"
  value       = aws_sqs_queue.dijkfood_orders_queue.arn
}