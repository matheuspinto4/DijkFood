import time
import requests
import math
import matplotlib.pyplot as plt
import contextily as ctx

# --- CONFIGURAÇÕES ---
API_URL = "http://dijkfood-api-alb-611216641.us-east-1.elb.amazonaws.com"
TAXA_ATUALIZACAO_SEGUNDOS = 1

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def obter_pedido_ativo():
    """Busca o pedido mais antigo que já está sendo transportado."""
    try:
        # Pede até 10.000 pedidos para garantir que vai enxergar a fila toda!
        pedidos = requests.get(f"{API_URL}/pedidos/?limit=10000").json()
        
        # Só pega o pedido se o entregador já estiver a caminho do cliente
        ativos = [p for p in pedidos if p['status'] in ['PICKED_UP', 'IN_TRANSIT']]
        
        if ativos:
            return ativos[0] # Pega o próximo da fila que já tem entregador
        return None
    except: 
        return None

def mapear_destinos(pedido):
    try:
        # Pede até 1.000 clientes e restaurantes para não perder ninguém de vista
        res = requests.get(f"{API_URL}/restaurantes/?limit=1000").json()
        cli = requests.get(f"{API_URL}/clientes/?limit=1000").json()
        
        r_data = next(r for r in res if r['id_restaurante'] == pedido['id_restaurante'])
        c_data = next(c for c in cli if c['id_cliente'] == pedido['id_cliente'])
        
        return {
            "res": (r_data['latitude'], r_data['longitude']),
            "cli": (c_data['latitude'], c_data['longitude']),
            "id": pedido['id_pedido']
        }
    except Exception as e:
        print(f"⚠️ Erro ao mapear destinos: {e}")
        return None

def main():
    plt.ion() # Modo interativo nativo
    fig, ax = plt.subplots(figsize=(10, 8))
    fig.canvas.manager.set_window_title('DijkFood - Rastreio Contínuo')

    while True:
        # --- ESTADO 1: PROCURANDO PEDIDO ---
        pedido = obter_pedido_ativo()
        
        if not pedido:
            ax.clear()
            ax.set_title("Aguardando novos pedidos na API...", fontsize=14)
            ax.set_xticks([])
            ax.set_yticks([])
            plt.pause(1)
            continue
        
        config = mapear_destinos(pedido)
        if not config:
            plt.pause(1)
            continue

        p_id = config['id']
        res_lat, res_lon = config['res']
        cli_lat, cli_lon = config['cli']

        print(f"\n🚀 Novo pedido detectado! Iniciando rastreio do Pedido #{p_id}")

        # --- ESTADO 2: PREPARANDO O MAPA PARA O NOVO PEDIDO ---
        ax.clear()
        ax.scatter([res_lon], [res_lat], c='orange', marker='s', s=200, label='Restaurante', zorder=3, edgecolors='black')
        ax.scatter([cli_lon], [cli_lat], c='green', marker='^', s=200, label='Cliente', zorder=3, edgecolors='black')
        
        # Inicia a bolinha no restaurante
        entregador_plot, = ax.plot([res_lon], [res_lat], 'bo', markersize=12, label='Entregador', zorder=4, markeredgecolor='white')
        
        # Ajusta a câmera inicial
        margem = 0.015
        ax.set_xlim(min(res_lon, cli_lon) - margem, max(res_lon, cli_lon) + margem)
        ax.set_ylim(min(res_lat, cli_lat) - margem, max(res_lat, cli_lat) + margem)
        
        try:
            ctx.add_basemap(ax, crs="EPSG:4326", source=ctx.providers.OpenStreetMap.Mapnik, alpha=0.7)
        except: pass

        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.legend(loc='upper right')
        ax.grid(True, linestyle='--', alpha=0.4)

        # --- ESTADO 3: RASTREANDO O PEDIDO ATÉ A ENTREGA ---
        entregue = False
        while not entregue:
            try:
                response = requests.get(f"{API_URL}/pedidos/{p_id}/acompanhamento")
                if response.status_code == 200:
                    dados = response.json()
                    status = dados.get("status")
                    pos = dados.get("posicao_entregador")
                    
                    if pos:
                        e_lat, e_lon = pos['latitude'], pos['longitude']
                        entregador_plot.set_data([e_lon], [e_lat])
                        
                        # Câmera Dinâmica
                        todas_lons = [res_lon, cli_lon, e_lon]
                        todas_lats = [res_lat, cli_lat, e_lat]
                        ax.set_xlim(min(todas_lons) - margem, max(todas_lons) + margem)
                        ax.set_ylim(min(todas_lats) - margem, max(todas_lats) + margem)
                        
                        if status in ["CONFIRMED", "PREPARING", "READY_FOR_PICKUP"]:
                            distancia = haversine(e_lat, e_lon, res_lat, res_lon)
                        else:
                            distancia = haversine(e_lat, e_lon, cli_lat, cli_lon)

                        tempo_seg = int(distancia / (500 / 3.6))
                        ax.set_title(f"Pedido #{p_id} | Status: {status} | Distância: {distancia:.1f}m | ETA: {tempo_seg}s")
                        
                    if status == "DELIVERED":
                        entregador_plot.set_data([cli_lon], [cli_lat])
                        ax.set_title(f"Pedido #{p_id} ENTREGUE! Preparando próximo...")
                        print(f"✅ Pedido #{p_id} entregue com sucesso!")
                        plt.pause(2) # Pausa de 2 segundos para você ler a tela antes de pular pro próximo
                        entregue = True # Quebra o loop menor e volta para buscar novos pedidos
                        
            except Exception as e:
                pass
            
            # Pausa para dar respiro à API e permitir que o Windows desenhe o gráfico
            plt.pause(TAXA_ATUALIZACAO_SEGUNDOS)

if __name__ == "__main__":
    main()