resource "aws_elasticache_subnet_group" "default" {
  name = "dijkfood-redis-subnet-group"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id = "dijkfood-redis"
  engine = "redis"
  node_type = "cache.t3.micro"
  num_cache_nodes = 1
  parameter_group_name = "default.redis7"
  port = 6379
  subnet_group_name = aws_elasticache_subnet_group.default.name
  security_group_ids = [aws_security_group.redis.id]
}