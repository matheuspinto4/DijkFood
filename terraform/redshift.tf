# --- redshift.tf ---

resource "aws_redshift_cluster" "analytics" {
  cluster_identifier  = "dijkfood-analytics-cluster"
  node_type           = "ra3.large"
  number_of_nodes     = 2
  
  database_name       = "dev"
  master_username     = "awsuser"
  master_password     = "SenhaForte2026!"
  
  # O Terraform encontrará o 'data.aws_iam_role.lab' que já existe no seu projeto
  iam_roles           = [data.aws_iam_role.lab.arn]
  
  publicly_accessible = true
  skip_final_snapshot = true

  # Ele buscará o SG criado lá no security_groups.tf
  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.analytics.endpoint
}