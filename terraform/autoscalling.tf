# API
resource "aws_appautoscaling_target" "api" {
  service_namespace = "ecs"
  resource_id = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.api.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity = var.api_min_instances
  max_capacity = var.api_max_instances
}

resource "aws_appautoscaling_policy" "api_cpu" {
  name = "api-cpu-scalling"
  service_namespace = "ecs"  
  resource_id = aws_appautoscaling_target.api.resource_id
  scalable_dimension = "ecs:service:DesiredCount"
  policy_type = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    target_value = 70.0
    scale_in_cooldown = 60
    scale_out_cooldown = 60

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAvarageCPUUtilization"
    }
  }
}

# WORKER
resource "aws_appautoscaling_target" "worker" {
  service_namespace = "ecs"
  resource_id = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity = var.worker_min_instances
  max_capacity = var.worker_max_instances
}

resource "aws_appautoscaling_policy" "worker_cpu" {
  name = "worker-cpu-scalling"
  service_namespace = "ecs"  
  resource_id = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = "ecs:service:DesiredCount"
  policy_type = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    target_value = 70.0
    scale_in_cooldown = 60
    scale_out_cooldown = 60

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAvarageCPUUtilization"
    }
  }
}
