# pyield-data

Dados históricos que a biblioteca [pyield](https://github.com/crdcj/pyield) baixa
em tempo de execução. Este repositório é infraestrutura interna da pyield: use a
API da biblioteca (`yd.tpf.taxas_historicas`, `yd.futuro.historico`), não os
arquivos diretamente. Nomes de arquivos e colunas podem mudar sem aviso.

## Datasets

Publicados como assets do [último release](https://github.com/crdcj/pyield-data/releases/latest):

| Arquivo | Conteúdo | Desde |
|---|---|---|
| `anbima_tpf.parquet` | Taxas indicativas e PU de títulos públicos federais (ANBIMA) | 02/01/2020 |
| `b3_futures.parquet` | Boletim de negociação da B3 para DI1, DDI, FRC, FRO, DAP, DOL, WDO, IND e WIN, com os nomes de coluna originais da B3 | 02/01/2018 |
| `vna_ntnb.parquet` | Legado, sem atualização desde 14/04/2025. A pyield calcula o VNA em `yd.vna` | — |

## Como a atualização funciona

- [`update.yml`](.github/workflows/update.yml) roda a cada 2 horas. O
  [`update.py`](update.py) busca nas fontes os pregões dos últimos 5 dias úteis que
  ainda não estão nos parquets e publica um novo release só quando algo mudou. O
  pregão do dia só é buscado depois das 22h BRT, quando as fontes já publicaram a
  versão definitiva.
- O processo é idempotente e não tem retentativa própria: fonte fora do ar vira
  log e a execução seguinte tenta de novo.
- [`check.yml`](.github/workflows/check.yml) roda uma vez por dia e é o único
  alarme: fica vermelho enquanto faltar algum pregão da janela.

## Rodando localmente

```sh
uv sync
gh release download --pattern "*.parquet" --dir release_staging
uv run update.py   # acrescenta os pregões que faltam
uv run check.py    # falha se faltar algum pregão da janela
```

As versões das bibliotecas usadas no job são as do `uv.lock`. Para atualizar:
`uv lock --upgrade` e commit.
