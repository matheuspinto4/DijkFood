data "archive_file" "processor" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/processor"
  output_path = "${path.module}/../lambda/processor.zip"
}

data "archive_file" "api_handler" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/api_handler"
  output_path = "${path.module}/../lambda/api_handler.zip"
}

resource "aws_lambda_function" "processor" {
  function_name    = "dijkfood-processor"
  filename         = data.archive_file.processor.output_path
  source_code_hash = data.archive_file.processor.output_base64sha256
  role             = data.aws_iam_role.lab.arn
  handler          = "processor.handler"
  runtime          = "python3.11"
  timeout          = 60

  vpc_config {
    subnet_ids         = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      REDIS_HOST = aws_elasticache_cluster.redis.cache_nodes[0].address
      REDIS_PORT = "6379"
    }
  }
}

resource "aws_lambda_function" "api_handler" {
  function_name    = "dijkfood-api-handler"
  filename         = data.archive_file.api_handler.output_path
  source_code_hash = data.archive_file.api_handler.output_base64sha256
  role             = data.aws_iam_role.lab.arn
  handler          = "api_handler.handler"
  runtime          = "python3.11"
  timeout          = 30

  vpc_config {
    subnet_ids         = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      REDIS_HOST = aws_elasticache_cluster.redis.cache_nodes[0].address
      REDIS_PORT = "6379"
    }
  }
}

resource "aws_lambda_event_source_mapping" "order_events" {
  event_source_arn  = aws_kinesis_stream.order_events.arn
  function_name     = aws_lambda_function.processor.arn
  starting_position = "LATEST"
}

resource "aws_lambda_event_source_mapping" "courier_positions" {
  event_source_arn  = aws_kinesis_stream.courier_positions.arn
  function_name     = aws_lambda_function.processor.arn
  starting_position = "LATEST"
}

resource "aws_lambda_event_source_mapping" "allocation_events" {
  event_source_arn  = aws_kinesis_stream.allocation_events.arn
  function_name     = aws_lambda_function.processor.arn
  starting_position = "LATEST"
}
