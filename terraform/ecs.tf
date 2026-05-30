data "aws_iam_role" "lab" {
    name = "LabRole"
}

resource "aws_ecs_cluster" "main" {
  name = var.ecs_cluster_name
}

resource "aws_ecs_task_definition" "api" {
  family = "dijkfood-api-task"
  network_mode = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu = "1024"
  memory = "2048"
  execution_role_arn = data.aws_iam_role.lab.arn
  task_role_arn = data.aws_iam_role.lab.arn

  container_definitions = jsonencode([{
    name = "dijkfood-api-container"
    image = var.api_image
    portMappings =[{
        containerPort = var.api_port
        hostPort = var.api_port
        protocol = "tcp"
    }]
    environment = [
        {name = "DB_HOST", value = aws_db_instance.postgres.address},
        {name = "DB_USER", value = var.db_user},
        {name = "DB_PASS", value = var.db_password},
        {name = "DB_NAME", value = var.db_name},
        {name = "DDB_EVENTOS", value = var.ddb_table_eventos},
        {name = "DDB_TELEMETRIA", value = var.ddb_table_telemetria},
    ]
    logConfiguration = {
        logDriver = "awslogs"
        options = {
            "awslogs-group" = "/ecs/dijkfood-api"
            "awslogs-region" = "us-east-1"
            "awslogs-stream-prefix" = "ecs"
            "awslogs-create-group" = "true"
        }
    }
  }])
}

resource "aws_ecs_service" "api" {
  name = "dijkfood-api-service"
  cluster = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count = 2
  launch_type = "FARGATE"

  network_configuration {
    subnets = data.aws_subnets.default.ids
    security_groups = [aws_security_group.api.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name = "dijkfood-api-container"
    container_port = var.api_port
  }

  depends_on = [aws_lb_listener.http]
}

resource "aws_ecs_task_definition" "worker" {
  family                   = "dijkfood-worker-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.worker_cpu)
  memory                   = tostring(var.worker_memory)
  execution_role_arn       = data.aws_iam_role.lab.arn
  task_role_arn            = data.aws_iam_role.lab.arn

  container_definitions = jsonencode([{
    name  = "dijkfood-worker-container"
    image = var.worker_image
    environment = [
      { name = "DB_HOST",       value = aws_db_instance.postgres.address },
      { name = "DB_USER",       value = var.db_user },
      { name = "DB_PASS",       value = var.db_password },
      { name = "DB_NAME",       value = var.db_name },
      { name = "DDB_EVENTOS",   value = var.ddb_table_eventos },
      { name = "DDB_ALOCACOES", value = var.ddb_table_alocacoes },
      { name = "S3_BUCKET",     value = aws_s3_bucket.grafo.id },
      { name = "API_URL",       value = "http://${aws_lb.api.dns_name}" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/dijkfood-worker"
        "awslogs-region"        = "us-east-1"
        "awslogs-stream-prefix" = "ecs"
        "awslogs-create-group"  = "true"
      }
    }
  }])
}

resource "aws_ecs_service" "worker" {
  name            = "dijkfood-worker-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.worker.id]
    assign_public_ip = true
  }
}