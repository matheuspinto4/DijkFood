"""
dashboard_check.py — Monitor em tempo real do API Gateway DijkFood.
Lê os endpoints de métricas e atualiza o terminal a cada N segundos.
"""

import subprocess
import requests
import time
import os
import sys

INTERVALO_SEGUNDOS = 3


def get_api_gateway_url():
    result = subprocess.run(
        ["terraform", "output", "-raw", "api_gateway_url"],
        capture_output=True, text=True, cwd="terraform"
    )
    if result.returncode != 0 or not result.stdout.strip():
        print("Erro: api_gateway_url não encontrado nos outputs do Terraform.")
        print("Rode 'terraform apply' primeiro.")
        sys.exit(1)
    return result.stdout.strip().rstrip("/")


def buscar(url, path):
    try:
        r = requests.get(f"{url}{path}", timeout=5)
        if r.status_code == 200:
            return r.json()
        return {"erro": f"HTTP {r.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"erro": "sem conexão"}
    except requests.exceptions.Timeout:
        return {"erro": "timeout"}
    except Exception as e:
        return {"erro": str(e)}


def limpar():
    os.system("cls" if os.name == "nt" else "clear")


def exibir(url, ciclo):
    orders     = buscar(url, "/metrics/orders")
    entregadores = buscar(url, "/metrics/entregadores")
    throughput = buscar(url, "/metrics/throughput")

    limpar()
    print("=" * 50)
    print("   DIJKFOOD — DASHBOARD TEMPO REAL")
    print(f"   Atualizado a cada {INTERVALO_SEGUNDOS}s  |  Ciclo #{ciclo}")
    print("=" * 50)

    print("\n📦  PEDIDOS")
    if "erro" in orders:
        print(f"   ⚠  {orders['erro']}")
    else:
        print(f"   Total processado : {orders.get('total', 0)}")
        for status, qtd in orders.get("por_status", {}).items():
            barra = "█" * min(qtd, 30)
            print(f"   {status:<20} {qtd:>5}  {barra}")

    print("\n🛵  ENTREGADORES")
    if "erro" in entregadores:
        print(f"   ⚠  {entregadores['erro']}")
    else:
        print(f"   Ativos agora     : {entregadores.get('ativos', 0)}")

    print("\n⚡  THROUGHPUT")
    if "erro" in throughput:
        print(f"   ⚠  {throughput['erro']}")
    else:
        print(f"   Pedidos/min      : {throughput.get('pedidos_ultimo_minuto', 0)}")
        print(f"   Alocações total  : {throughput.get('alocacoes_total', 0)}")

    print("\n" + "=" * 50)
    print(f"   API Gateway: {url}")
    print("   Ctrl+C para sair")
    print("=" * 50)


def main():
    print("Buscando URL do API Gateway...")
    url = get_api_gateway_url()
    print(f"Conectando em: {url}")
    time.sleep(1)

    ciclo = 1
    while True:
        exibir(url, ciclo)
        ciclo += 1
        time.sleep(INTERVALO_SEGUNDOS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nMonitor encerrado.")