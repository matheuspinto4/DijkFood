resource "aws_db_parameter_group" "postgres16" {
    name = "dijkfood-pg16"
    family = "postgres16"
}

resource "aws_db_subnet_group" "default" {
    name = "dijkfood-subnet-gorup"
    subnet_ids = data.aws_subnets.default.ids
}

resource "aws_db_instance" "postgres" {
    identifier = var.db_instace_id
    engine = "postgres"
    engine_version = var.pg_version
    instance_class = var.db_instance_class
    allocated_storage = 20

    db_name = var.db_name
    username = var.db_user
    password = var.db_password

    parameter_group_name = aws_db_parameter_group.postgres16.name
    db_subnet_group_name = aws_db_subnet_group.default.name
    vpc_security_group_ids = [aws_security_group.rds.id]

    multi_az = true
    publicly_accessible = true
    skip_final_snapshot = true
}