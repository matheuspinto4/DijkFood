# RDS
variable "db_instace_id" {
    description = "Identificador de intância RDS"
    type = string
    default = "dijkfood-primary"
}

variable "db_name" {
    description = "Nome do banco PostgreSQL"
    type = string
    default = "dijkfooddb"
}

variable "db_user" {
  description = "Usuário administrador do banco"
  type        = string
  default     = "dijk_admin"
}

variable "db_password" {
  description = "Senha do banco de dados"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t3.micro"
}

variable "pg_version" {
  type    = string
  default = "16"
}


# DynamoDB 
variable "ddb_table_eventos" {
  type    = string
  default = "dijkfood-historico-eventos"
}

variable "ddb_table_telemetria" {
  type    = string
  default = "dijkfood-telemetria-entregadores"
}

variable "ddb_table_alocacoes" {
  type    = string
  default = "dijkfood-alocacao-entregadores"
}

# ECS / Fargate 
variable "ecs_cluster_name" {
  type    = string
  default = "DijkFoodCluster"
}

variable "api_image" {
  type    = string
  default = "matheuspinto4/dijkfood-api:latest"
}

variable "worker_image" {
  type    = string
  default = "matheuspinto4/dijkfood-worker:latest"
}

variable "api_port" {
  type    = number
  default = 80
}

variable "worker_cpu" {
  type    = number
  default = 1024
}

variable "worker_memory" {
  type    = number
  default = 2048
}

# Auto Scaling
variable "api_min_instances" {
  type    = number
  default = 2
}

variable "api_max_instances" {
  type    = number
  default = 30
}

variable "worker_min_instances" {
  type    = number
  default = 2
}

variable "worker_max_instances" {
  type    = number
  default = 30
}