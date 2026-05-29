resource "aws_lb" "api" {
    name = "dijkfood-api-alb"
    internal = false
    load_balancer_type = "application"
    security_groups = [aws_security_group.alb.id]
    subnets = data.aws_subnets.default.ids
}

resource "aws_lb_target_group" "api" {
    name = "dijkfood-api-tg"
    port = var.api_port
    protocol = "HTTP"
    vpc_id = data.aws_vpc.default.id
    target_type = "ip"

    health_check {
      path = "/"
    }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.api.arn
  port = 80
  protocol = "HTTP"

  default_action {
    type = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}