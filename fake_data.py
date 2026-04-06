from faker import Faker
from faker.providers import BaseProvider
import random
import pandas as pd

class RestauranteProvider(BaseProvider):
    tipos_culinaria = [
        'Pizzaria', 'Churrascaria', 'Comida Japonesa', 'Comida Mineira', 
        'Comida Baiana', 'Lanchonete', 'Hamburgueria', 'Cantina Italiana', 
        'Comida Vegana', 'Bistrô Francês', 'Frutos do Mar'
    ]

    prefixos = ['Restaurante', 'Cantina', 'Pizzaria', 'Bar e Petiscaria', 'Recanto', 'Espaço']
    sufixos = ['Gourmet', 'da Família', 'Tradicional', 'Express', 'Saboroso', 'Grill']

    def tipo_restaurante(self):
        """Retorna uma categoria de restaurante aleatória."""
        return self.random_element(self.tipos_culinaria)

    def nome_restaurante(self):
        """Gera um nome de restaurante combinando palavras ou usando nomes de pessoas."""
        formato = random.choice(['nome_pessoa', 'prefixo_sufixo', 'sobrenome'])
        
        if formato == 'nome_pessoa':
            # Ex: Bar do João, Restaurante da Maria
            artigo = self.random_element(['do', 'da'])
            nome = self.generator.first_name()
            estabelecimento = self.random_element(['Bar', 'Restaurante', 'Cantina', 'Lanchonete'])
            return f"{estabelecimento} {artigo} {nome}"
            
        elif formato == 'prefixo_sufixo':
            # Ex: Restaurante Gourmet, Recanto Tradicional
            return f"{self.random_element(self.prefixos)} {self.random_element(self.sufixos)}"
            
        else:
            # Ex: Cantina Silva, Pizzaria Oliveira
            estabelecimento = self.random_element(self.prefixos)
            sobrenome = self.generator.last_name()
            return f"{estabelecimento} {sobrenome}"


def get_lat_lon(nodes): 
    return random.choice(nodes.values)[1:]

def gerar_dados_falsos(numero_de_clientes : int, numero_de_restaurantes : int, numero_de_entregadores : int):
    """
    Gera dados falsos de clientes, restaurantes e entregadores para popular o RDS.
    """
    fake = Faker(['pt-BR'])
    fake.add_provider(RestauranteProvider)
    nodes = pd.read_csv("nodes.csv")

    clientes = [
        {
            "nome": f"{fake.name()}", 
            "email": f"{fake.email()}", 
            "telefone": f"{fake.phone_number()}", 
            **{k: v for k, v in zip(["latitude", "longitude"], get_lat_lon(nodes))}
        } for _ in range(numero_de_clientes)
    ]
    restaurantes = [
        {
            "nome": f"{fake.nome_restaurante()}", 
            "tipo_cozinha": f"{fake.tipo_restaurante()}", 
            **{k: v for k, v in zip(["latitude", "longitude"], get_lat_lon(nodes))}
        } for _ in range(numero_de_restaurantes)
    ]
    entregadores = [
        {
            "nome": f"{fake.name()}", 
            "tipo_veiculo": f"{random.choice(['moto', 'carro', 'caminhao', 'biscicleta', 'pé', 'cavalo', 'triciclo', 'chihuahua', 'galinha'])}", 
            **{k: v for k, v in zip(["latitude", "longitude"], get_lat_lon(nodes))}
        } for _ in range(numero_de_entregadores)
    ]

    return (clientes, restaurantes, entregadores)

if __name__ == "__main__":

    A = gerar_dados_falsos(10,20, 30)    
    
    for a in A:
        for l in a:
            print(l)
        print(40*"_")