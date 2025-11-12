import requests
import xmltodict
from datetime import datetime

CONFIG_FILE = "config.xml"
LOG_FILE = "tracking_sync.log"

# Ler configuração XML
with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = xmltodict.parse(f.read())["multpex_config"]

SERVIDOR_AUTENTICACAO = config["SERVIDOR_AUTENTICACAO"]
SERVIDOR_APLICACAO = config["SERVIDOR_APLICACAO"]
CLIENT_ID = config["CLIENT_ID"]
CLIENT_SECRET = config["CLIENT_SECRET"]
USERNAME = config["USERNAME"]
PASSWORD = config["PASSWORD"]

import os
from dotenv import load_dotenv

load_dotenv()

EAM_URL = os.getenv("EAM_URL")
EAM_USER = os.getenv("EAM_USER")
EAM_PASS = os.getenv("EAM_PASS")
EAM_TENANT = os.getenv("EAM_TENANT")
EAM_ORG = os.getenv("EAM_ORG")


# Utilitário de log
def log(msg):
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().isoformat()}] {msg}\n")

# Autenticar no Keycloak e obter token
def autenticar():
    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": USERNAME,
        "password": PASSWORD,
    }
    resp = requests.post(SERVIDOR_AUTENTICACAO, data=payload)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    log("Token obtido com sucesso!")
    return token

# Buscar dados da API Multipex
def obter_dados(token):
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(SERVIDOR_APLICACAO, headers=headers)
    resp.raise_for_status()
    dados = resp.json()
    registros = dados.get("results", {}).get("data", [])
    log(f"Total de registros obtidos: {len(registros)}")
    return registros

# Enviar via SOAP para o EAM
def enviar_para_eam(registros):
    headers = {
        "Content-Type": "text/xml;charset=UTF-8",
        "SOAPAction": "AddInterfaceTransactions"
    }

    sucesso, falhas = 0, 0

    for idx, r in enumerate(registros, 1):
        req_num = r.get("RequisicaoNumero") or 0
        item_cod = r.get("ItemCodigo") or "ITEM-INVALIDO"
        item_qtd = r.get("ItemQuantidade") or 0
        data_fin = r.get("FinalizacaoData") or "DATA-INVALIDA"
        unidade = r.get("EntregaUnidadeNome") or "NAVIO_DESCONHECIDO"

        if not req_num or not item_cod:
            log(f"Ignorando registro sem número/item ({req_num}/{item_cod})")
            continue

        xml = f"""<?xml version="1.0" encoding="utf-8"?>
<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Header>
    <Security xmlns="http://schemas.xmlsoap.org/ws/2002/04/secext">
      <UsernameToken>
        <Username>{EAM_USER}@{EAM_TENANT}</Username>
        <Password>{EAM_PASS}</Password>
      </UsernameToken>
    </Security>
    <SessionScenario xmlns="http://schemas.datastream.net/headers">terminate</SessionScenario>
    <Organization xmlns="http://schemas.datastream.net/headers">{EAM_ORG}</Organization>
  </Header>
  <Body>
    <MP0810_AddInterfaceTransactions_001 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://schemas.datastream.net/MP_functions/MP0810_001"
        verb="Add" noun="InterfaceTransactions" version="001" callname="AddInterfaceTransactions">
      <InterfaceTransactions xmlns="http://schemas.datastream.net/MP_entities/InterfaceTransactions_001">
        <TrackingData xmlns="http://schemas.datastream.net/MP_entities/TrackingData_001">
          <SOURCESYSTEM xmlns="http://schemas.datastream.net/MP_fields">MULTI</SOURCESYSTEM>
          <SOURCECODE xmlns="http://schemas.datastream.net/MP_fields">APIT</SOURCECODE>
          <TRANSCODE xmlns="http://schemas.datastream.net/MP_fields">M201</TRANSCODE>
          <SESSIONID xmlns="http://schemas.datastream.net/MP_fields">102</SESSIONID>
          <CHANGED xmlns="http://schemas.datastream.net/MP_fields">PYTHO</CHANGED>
          <PROMPTDATA1 xmlns="http://schemas.datastream.net/MP_fields">MULTIPEX</PROMPTDATA1>
          <PROMPTDATA2 xmlns="http://schemas.datastream.net/MP_fields">{req_num}</PROMPTDATA2>
          <PROMPTDATA3 xmlns="http://schemas.datastream.net/MP_fields">{item_cod}</PROMPTDATA3>
          <PROMPTDATA4 xmlns="http://schemas.datastream.net/MP_fields">{item_qtd}</PROMPTDATA4>
          <PROMPTDATA5 xmlns="http://schemas.datastream.net/MP_fields">{data_fin}</PROMPTDATA5>
          <PROMPTDATA6 xmlns="http://schemas.datastream.net/MP_fields">{unidade}</PROMPTDATA6>
        </TrackingData>
      </InterfaceTransactions>
    </MP0810_AddInterfaceTransactions_001>
  </Body>
</Envelope>"""

        log(f"[{idx}/{len(registros)}]Numero-Req:{req_num}, Codigo-Item:{item_cod}, Qtd:{item_qtd}, Navio:{unidade}, Data-Finalizacao:{data_fin}")
        try:
            resp = requests.post(EAM_URL, data=xml.encode("utf-8"), headers=headers, timeout=30)
            if resp.status_code == 200 and "<TRANSID" in resp.text:
                log(f"✅ Sucesso Req={req_num} | TRANSID criado.")
                sucesso += 1
            else:
                log(f"HTTP {resp.status_code}")
                falhas += 1
        except Exception as e:
            log(f"Falha Req={req_num}: {e}")
            falhas += 1

    log("Sincronização finalizada.")

# Função principal
def main():
    token = autenticar()
    registros = obter_dados(token)
    enviar_para_eam(registros)

# Execução
if __name__ == "__main__":
    main()
