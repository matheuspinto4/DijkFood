resource "aws_dynamodb_table" "eventos" {
    name = var.ddb_table_eventos
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "id_pedido" # Chave primária
    range_key = "timestamp" # Chave composta

    attribute {
      name = "id_pedido"
      type = "S" # Tipo string
    }

    attribute {
      name = "timestamp"
      type = "S"
    }
}

resource "aws_dynamodb_table" "telemetria" {
    name = var.ddb_table_telemetria
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "id_entregador"
    range_key = "timestamp"

    attribute {
      name = "id_entregador"
      type = "S"
    }

    attribute {
      name = "timestamp"
      type = "S"
    }
}

resource "aws_dynamodb_table" "alocacoes" {
    name = var.ddb_table_alocacoes
    billing_mode = "PAY_PER_REQUEST"
    hash_key = "id_entregador"
    range_key = "timestamp"

    attribute {
      name = "id_entregador"
      type = "S"
    }

    attribute {
      name = "timestamp"
      type = "S"
    }
}