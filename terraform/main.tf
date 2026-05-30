terraform {
    #  Alocando o provedor
    required_providers {
       aws = {
        source = "hashicorp/aws"
        version = "~> 5.0"
       }
       archive = {
        source = "hashicorp/archive"
        version = "~> 2.0"
       }
    }
}

# Configurando o provedor
provider "aws" {
    region = "us-east-1"
}

# Buscando a VPC e Subnets 
# Buscando os dados que já existem na AWS
data "aws_vpc" "default" {
    default = true
}

data "aws_subnets" "default" {
    filter {
        name = "vpc-id"
        values = [data.aws_vpc.default.id] # Filtra pelo ID da VPC
    }
}

data "aws_caller_identity" "current" {}