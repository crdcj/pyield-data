import io
import logging
import zipfile
from datetime import date

import polars as pl
import requests
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

NS = {"ns": "urn:bvmf.217.01.xsd"}

# Contratos de interesse (prefixo de 3 chars do ticker)
# Baseado em pyield.b3.futures, incluindo FRO por regra do projeto.
ASSET_CODES = {"DI1", "DDI", "FRC", "FRO", "DAP", "DOL", "WDO", "IND", "WIN"}


# Colunas ativas do consolidado (chave=nome da coluna, valor=tipo polars).
# O código do PDF fica no comentário ao final de cada linha.
TIPOS = {
    "TradDt": pl.Date,  # 1.00: Data de negociação
    "TckrSymb": pl.String,  # 2.01: Código instrumento
    # "Id": pl.String,  # 3.01.01: Identificação instrumento
    # "Prtry": pl.String,  # 3.01.02.01: Tipo identificação (próprio)
    # "MktIdrCd": pl.String,  # 3.02.01: Código identificador mercado
    # "DaysToSttlm": pl.String,  # 4.01: Dias para liquidação (contrato a termo)
    "TradQty": pl.Int64,  # 4.02: Qtd negócios (TradDtls)
    # "MktDataStrmId": pl.String,  # 5.01: Fluxo preço
    "NtlFinVol": pl.Float64,  # 5.02: Volume financeiro R$
    # "IntlFinVol": pl.Float64,  # 5.03: Volume financeiro US$
    "OpnIntrst": pl.Int64,  # 5.04: Contratos em aberto
    "FinInstrmQty": pl.Int64,  # 5.05: Qtd negociada no dia
    "BestBidPric": pl.Float64,  # 5.06: Melhor oferta compra
    "BestAskPric": pl.Float64,  # 5.07: Melhor oferta venda
    "FrstPric": pl.Float64,  # 5.08: Primeiro preço do dia
    "MinPric": pl.Float64,  # 5.09: Preço mínimo do dia
    "MaxPric": pl.Float64,  # 5.10: Preço máximo do dia
    "TradAvrgPric": pl.Float64,  # 5.11: Preço médio do dia
    "LastPric": pl.Float64,  # 5.12: Último preço do dia
    # "RglrTxsQty": pl.Int64,  # 5.13: Qtd negócios regular
    # "NonRglrTxsQty": pl.Int64,  # 5.14: Qtd negócios não regular
    # "RglrTraddCtrcts": pl.Int64,  # 5.15: Qtd contratos regular
    # "NonRglrTraddCtrcts": pl.Int64,  # 5.16: Qtd contratos não regular
    # "NtlRglrVol": pl.Float64,  # 5.17: Volume R$ regular
    # "NtlNonRglrVol": pl.Float64,  # 5.18: Volume R$ não regular
    # "IntlRglrVol": pl.Float64,  # 5.19: Volume US$ regular
    # "IntlNonRglrVol": pl.Float64,  # 5.20: Volume US$ não regular
    "AdjstdQt": pl.Float64,  # 5.21: Cotação de ajuste
    "AdjstdQtTax": pl.Float64,  # 5.22: Cotação de ajuste taxa
    # "AdjstdQtStin": pl.String,  # 5.23: Situação ajuste dia
    # "PrvsAdjstdQt": pl.Float64,  # 5.24: Cotação ajuste dia anterior
    # "PrvsAdjstdQtTax": pl.Float64,  # 5.25: Cotação ajuste taxa anterior
    # "PrvsAdjstdQtStin": pl.String,  # 5.26: Situação ajuste anterior
    # "OscnPctg": pl.Float64,  # 5.27: Percentual oscilação
    # "VartnPts": pl.Float64,  # 5.28: Diferença ajuste anterior
    # "EqvtVal": pl.Float64,  # 5.29: Valor equivalente
    "AdjstdValCtrct": pl.Float64,  # 5.30: Valor ajuste por contrato
    "MaxTradLmt": pl.Float64,  # 5.31: Limite máximo negociação
    "MinTradLmt": pl.Float64,  # 5.32: Limite mínimo negociação
}

MAPA_RENOMEACAO_DATASET_PR = {
    "TradDt": "TradeDate",
    "TckrSymb": "TickerSymbol",
    "TradQty": "TradeCount",
    "FinInstrmQty": "TradeVolume",
    "NtlFinVol": "FinancialVolume",
    "OpnIntrst": "OpenContracts",
    "BestBidPric": "BestBidValue",
    "BestAskPric": "BestAskValue",
    "FrstPric": "OpenValue",
    "MinPric": "MinValue",
    "MaxPric": "MaxValue",
    "TradAvrgPric": "AvgValue",
    "LastPric": "CloseValue",
    "AdjstdQt": "SettlementPrice",
    "AdjstdQtTax": "SettlementRate",
    "AdjstdValCtrct": "AdjustedValueContract",
    "MaxTradLmt": "MaxLimitValue",
    "MinTradLmt": "MinLimitValue",
}


def _url(trade_date: date) -> str:
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{trade_date:%y%m%d}.zip"


def _extrair_xml(conteudo_zip: bytes) -> bytes:
    """Extrai o XML mais recente de dentro do ZIP aninhado (zip > zip > xml)."""
    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as zf_ext:
        with zipfile.ZipFile(io.BytesIO(zf_ext.read(zf_ext.namelist()[0]))) as zf_int:
            xmls = [i for i in zf_int.infolist() if i.filename.lower().endswith(".xml")]
            mais_recente = max(xmls, key=lambda i: (i.date_time, i.filename))
            return zf_int.read(mais_recente.filename)


def baixar(trade_date: date) -> bytes | None:
    """Baixa o PR da data e retorna o XML em memória."""

    try:
        resp = requests.get(_url(trade_date), timeout=(5, 10))
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.warning("Falha ao baixar %s: %s", trade_date, e)
        return

    if len(resp.content) < 1024:
        log.info("Sem dados para %s", trade_date)
        return

    xml = _extrair_xml(resp.content)
    log.info("XML carregado em memória para %s", trade_date)
    return xml


def _ticker_valido(ticker: str) -> bool:
    # Comprimento válido do ticker por prefixo (padrão: 6).
    # CPM (opções sobre copom): 13 chars (ex: CPMF25C100750)
    tamanho_por_prefixo = {"CPM": 13}
    tamanho_padrao = 6

    prefixo = ticker[:3]
    tamanho_esperado = tamanho_por_prefixo.get(prefixo, tamanho_padrao)
    return len(ticker) == tamanho_esperado


def _extrair_registro(
    ticker_elem: etree._Element, ticker: str
) -> dict[str, str | None] | None:
    trade_date_xpath = ".//ns:TradDt/ns:Dt"
    trade_qty_xpath = ".//ns:TradDtls/ns:TradQty"
    fin_instrm_attrbts_xpath = ".//ns:FinInstrmAttrbts"

    # TckrSymb → FinInstrmId → PricRpt
    report = ticker_elem.getparent()
    if report is not None:
        report = report.getparent()
    if report is None:
        return None

    date_elem = report.find(trade_date_xpath, NS)
    if date_elem is None or date_elem.text is None:
        return None

    trade_qty_elem = report.find(trade_qty_xpath, NS)

    attrs_elem = report.find(fin_instrm_attrbts_xpath, NS)
    if attrs_elem is None:
        return None

    registro: dict[str, str | None] = {
        "TradDt": date_elem.text,
        "TckrSymb": ticker,
        "TradQty": trade_qty_elem.text if trade_qty_elem is not None else None,
    }
    for attr in attrs_elem:
        registro[etree.QName(attr).localname] = attr.text

    return registro


def _parse_xml(xml_bytes: bytes) -> list[dict]:
    """Parseia o XML uma vez e extrai registros dos contratos de interesse."""
    parser = etree.XMLParser(
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        recover=True,
        resolve_entities=False,
        no_network=True,
    )
    tree = etree.parse(io.BytesIO(xml_bytes), parser=parser)
    tickers = tree.xpath("//ns:TckrSymb", namespaces=NS)
    if not tickers or not isinstance(tickers, list):
        return []

    registros = []
    for ticker_elem in tickers:
        if not isinstance(ticker_elem, etree._Element):
            continue
        texto = ticker_elem.text
        if not texto or texto[:3] not in ASSET_CODES:
            continue
        if not _ticker_valido(texto):
            continue

        registro = _extrair_registro(ticker_elem, texto)
        if registro is not None:
            registros.append(registro)

    return registros


def parse_price_report(file_bytes: bytes) -> pl.DataFrame:
    """Parseia bytes de XML já descomprimidos e retorna o dataframe consolidado."""
    registros = _parse_xml(file_bytes)

    if not registros:
        log.warning("Nenhum registro encontrado.")
        return pl.DataFrame()

    df = pl.DataFrame(registros)
    selected_cols = [c for c in TIPOS if c in df.columns]
    tipos_presentes = {c: TIPOS[c] for c in selected_cols}

    df = (
        df.select(selected_cols)
        .cast(tipos_presentes, strict=False)  # pyright: ignore[reportArgumentType]
        .sort("TckrSymb", "TradDt")
        .rename(MAPA_RENOMEACAO_DATASET_PR, strict=False)
    )
    log.info("Shape final: %s", df.shape)

    return df


def fetch_price_report_by_date(trade_date: date) -> pl.DataFrame:
    """Baixa o PR por data, carrega em memória e retorna o dataframe consolidado."""
    xml_bytes = baixar(trade_date)
    if xml_bytes is None:
        # Retorna DataFrame vazio para sinalizar ausência de dados
        return pl.DataFrame()

    return parse_price_report(xml_bytes)
