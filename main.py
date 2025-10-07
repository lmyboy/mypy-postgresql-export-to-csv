import psycopg2
import hashlib
from psycopg2.extras import RealDictCursor
import dotenv
import time
import csv
import subprocess
import datetime
import zipfile
import os
from os import listdir
from os.path import isfile, join, dirname
import zipfile

dotenv.load_dotenv()

search = [
            ("abastecimentos_setembro_2025",
             f"""
                SELECT 
    ap.CD_AUTORIZACAO_PAGAMENTO "ID ABASTECIMENTO",
    ap.CD_TRANSACAO_FROTA "TRANSAÇÃO FROTA",
    to_char(ap.dt_requisicao, 'YYYY-MM-DD') as "DATA ABASTECIMENTO",
    to_char(ap.dt_processamento, 'YYYY-MM-DD') as "DATA TRANSAÇÃO",
    pv.CD_PTOV_ABADI "ABADI",
    ap.CD_CNPJ_AREA_ABASTECIMENTO "CNPJ POSTO",
    ap.nm_ptov "RAZÃO SOCIAL POSTO",
    pv.NM_MUNIC "MUNICÍPIO POSTO",
    pv.SG_UF "UF POSTO",
    b.DS_BAND "BANDEIRA",
    ap.CD_CNPJ_FROTA "CNPJ FROTA",
    f.NM_RAZAO_SOCIAL "RAZÃO SOCIAL FROTA",
    f.nm_munic "MUNICÍPIO FROTA",
    f.SG_UF "UF FROTA",
    to_char(ap.CD_CPF_MOTORISTA, '99999999999') as "CPF MOTORISTA",
    ap.NM_MOTORISTA "NOME MOTORISTA",
    ap.DS_PLACA_VEICULO "PLACA",
    ap.qt_total_lit_abas "LITROS",
    ap.NM_ITEM_ABAS "COMBUSTÍVEL",
    pp.va_preco "PREÇO BASE",
    ap.VA_UNITARIO_ABAS "VALOR UNITÁRIO",
    ap.VA_TOTAL "TOTAL",
    tc2.MDR "MDR",
    round(((ap.va_total * tc2.mdr) / 100), 3) "TAXA PRÓ-FROTAS",
    CASE
        ap.ID_STATUS 
        WHEN (1) THEN 'AUTORIZADO'
        WHEN (-1) THEN 'CANCELADO' 
    END
    "STATUS",
    ap.DS_MOTIVO_RECUSA "MOTIVO DA RECUSA",
    CASE
        ap.id_tipo_auto_pag 
        WHEN (1) THEN 'PDV'
        WHEN (2) THEN 'PCC'
        WHEN (3) THEN 'Inclusão Manual'
        WHEN (4) THEN 'Comanda de Posto Interno'
        WHEN (5) THEN 'Comanda Digital'
        WHEN (6) THEN 'PDV WEB'
WHEN (7) THEN 'POS'
        WHEN (8) THEN 'MAN PI'
        WHEN (9) THEN 'CTA PLUS'
        WHEN (10) THEN 'POS FL'
        WHEN (11) THEN 'Automação'
        WHEN (12) THEN 'Automação PDV Móvel'
        WHEN (13) THEN 'Api Revenda'
        ELSE '-'
    END "ORIGEM PAGAMENTO",
    CASE
        WHEN (ap.CD_PEDIDO IS NOT NULL AND ap.ID_TIPO_SENHA_AUTO = 2) THEN 'ONLINE'
        ELSE 'OFFLINE'
    END "PEDIDO",
    case WHEN iapg.qt_item IS NULL THEN 0 else iapg.qt_item END "QUANT. ARLA GRANEL",
    CASE WHEN iapg.nm_item IS NULL THEN '-' ELSE iapg.nm_item END "TIPO ARLA GRANEL",
    CASE WHEN iapg.va_total IS NULL THEN 0 ELSE iapg.va_total END "VALOR TOTAL ARLA GRANEL",
    CASE WHEN iapb.qt_item IS NULL THEN 0 ELSE iapb.qt_item END "QUANT. ARLA BALDE",
    CASE WHEN iapb.nm_item IS NULL THEN '-' ELSE iapb.nm_item END "TIPO ARLA BALDE",
    CASE WHEN iapb.va_total IS NULL THEN 0 ELSE iapb.va_total END "VALOR TOTAL ARLA BALDE",
    ap.NO_HODOMETRO "HODÔMETRO",
    ap.NO_HODOMETRO_ANT "HODOMETRO ANTERIOR",
    ap.NO_HORIMETRO "HORÍMETRO",
    ap.NO_HORIMETRO_ANT "HORIMETRO ANTERIOR",
    ea.CD_CNPJ "CNPJ EMPRESA AGREGADA",
    ea.NM_RAZAO_SOCIAL "RAZAO SOCIAL EMPRESA AGREGADA",
    ea.NM_MUNIC "MUNICÍPIO EMPRESA AGREGADA",
    ea.SG_UF "UF EMPRESA AGREGADA"
FROM
    boleia_schema.autorizacao_pagamento ap LEFT JOIN boleia_schema.tipo_combustivel tc ON tc.ds_tipo_combustivel = ap.NM_ITEM_ABAS 
LEFT JOIN (
SELECT max(pp.dt_atualizacao) dt_atualizacao, pp.VA_PRECO, pp.cd_ptov, mp.cd_tipo_combustivel 
FROM boleia_schema.PTOV_PRECO pp 
JOIN boleia_schema.micromercado_preco mp ON mp.cd_micromercado_preco = pp.CD_MICROMERCADO_PRECO
WHERE pp.id_status = 3
group by pp.cd_ptov, pp.va_preco, mp.cd_tipo_combustivel limit 1
    ) pp ON pp.cd_ptov = ap.cd_ptov AND pp.cd_tipo_combustivel = tc.cd_tipo_combustivel and pp.dt_atualizacao < ap.dt_requisicao --ajustado para a tentar nao duplicar o preco base
LEFT JOIN boleia_schema.item_autorizacao_pagamento iapg ON
    ap.cd_autorizacao_pagamento = iapg.cd_autorizacao_pagamento
    AND iapg.id_tipo = 2
    AND iapg.nm_item LIKE '%Arla 32 - Granel%'
LEFT JOIN boleia_schema.item_autorizacao_pagamento iapb ON
    ap.cd_autorizacao_pagamento = iapb.cd_autorizacao_pagamento
    AND iapb.id_tipo = 2
    AND iapb.nm_item LIKE '%Arla 32 - Balde%'
LEFT JOIN BOLEIA_SCHEMA.autorizacao_pgto_edicao ape
    ON
    ape.cd_autorizacao_pagamento = ap.cd_autorizacao_pagamento
LEFT JOIN boleia_schema.PONTO_VENDA pv
    ON
    ap.cd_ptov = pv.cd_ptov
LEFT JOIN boleia_schema.COMPONENTE c
    ON
    c.CD_PTOV = pv.cd_ptov
LEFT JOIN boleia_schema.BANDEIRA b
    ON
    c.CD_BAND = b.CD_BAND
LEFT JOIN boleia_schema.frota f
    ON
    ap.cd_cnpj_frota = f.cd_cnpj
LEFT JOIN BOLEIA_SCHEMA.EMPRESA_AGREGADA ea
    ON
    ap.CD_EMPR_AGREGADA = ea.CD_EMPR_AGREGADA
LEFT JOIN boleia_schema.trans_consol tc2 ON
    ( ( ap.cd_trans_consol = tc2.cd_trans_consol
        AND ap.cd_trans_consol_postergada IS NULL )
    OR ( ap.cd_trans_consol_postergada = tc2.cd_trans_consol ) )
WHERE 1=1
    and date_trunc('day', dt_processamento) between to_date('2025-09-01','YYYY-MM-DD') and to_date('2025-09-30','YYYY-MM-DD')
    AND ap.id_status IN (1,-1)
    AND f.ID_EXCLUIDO = 0
    --and ap.cd_ptov in (5557,15879,5390,545,3037)
    and (iapg.qt_item <> 0 or iapb.qt_item <> 0)
    --and ap.cd_frota = 1978
    AND ap.cd_cnpj_frota NOT IN (44398528000136)
    AND c.CD_ATIV_COMP IN (2, 33) -- (SELECT cd_ativ_comp from boleia_schema.ATIVIDADE_COMPONENTE where cd_ativ_comp_corp::numeric IN (1,99))
    AND (ape.cd_autorizacao_pgto_edicao IS NULL
        OR ape.cd_autorizacao_pgto_edicao = (
        SELECT
            max(ape_.cd_autorizacao_pgto_edicao)
        FROM
            BOLEIA_SCHEMA.autorizacao_pgto_edicao ape_
        WHERE
            ape_.cd_autorizacao_pagamento = ap.cd_autorizacao_pagamento
            AND ape_.id_status_edicao = 1
        )
    )
             """)
            ]


def run_query(search, foler_name):
    for row in search:
        sql_name = row[0].__str__()
        sql = row[1].__str__()

        dir_path = os.path.dirname(os.path.realpath(__file__))

        if os.path.isfile(dir_path):
            os.remove(f"{sql_name}.csv")
            print(f"{sql_name}.csv deleted.")
        else:
            print(f"{sql_name}.csv does not exist yet.")

        csv_file = os.path.join(dir_path, foler_name, f"{sql_name}.csv")

        print(f"--- Running query: {sql_name}.sql ---")
        start_time = time.time()
        output_query = (f"""--pyReport
                        COPY ({sql}) TO STDOUT WITH (FORMAT CSV, HEADER true, DELIMITER ';', FORCE_QUOTE *)""")

        start_time_step = time.perf_counter()

        start_timestamp = datetime.datetime.now()

        with psycopg2.connect(
                host=os.environ.get('DB_HOST'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                dbname=os.environ.get('DB_NAME'),
                port=os.environ.get('DB_PORT'),
        ) as conn:
            with conn.cursor() as cur:
                with open(csv_file, "w", encoding="utf-8-sig") as f:
                    cur.copy_expert(output_query, f)

        print(f"--- {(time.time() - start_time)} seconds to run {sql_name}.sql ---\n")
        start_time = time.time()

        with open(csv_file, "rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)


        # optional: generate md5 hash file and zip file
        # print(f"--- {csv_file} MD5: {file_hash.hexdigest()} ---")
        # open(f"{csv_file}.md5", "w").write(f"{file_hash.hexdigest()} at: {start_timestamp}")
        # print(f"--- {(time.time() - start_time)} seconds to generate MD5 hash for file {csv_file} ---\n")
        #
        # ##compacting csv file
        # zip = zipfile.ZipFile(f"{csv_file}-python.zip", "w", zipfile.ZIP_DEFLATED)
        # zip.write(f"{csv_file}")
        # zip.close()
        #
        # print(f"--- {csv_file}-python.zip MD5: {file_hash.hexdigest()} ---")
        # open(f"{csv_file}-python.zip.md5", "w").write(f"{file_hash.hexdigest()} at: {start_timestamp}")
        # print(f"--- {(time.time() - start_time)} seconds to generate MD5 hash for file {csv_file} ---\n")


def main():
    run_query(search, "output")

if __name__ == "__main__":
    main()