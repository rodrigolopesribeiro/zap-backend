# Busca de imovel no Zap por endereco parcial ou condominio

## Visao geral

Este projeto oferece:
- CLI em Python + Playwright para varrer listagens do Zap Imoveis.
- API Flask local para consumo por frontend e orquestradores (ex.: n8n).
- Filtros por endereco, condominio e recencia.

Regra central de paginacao:
- Cada execucao processa no maximo **2 paginas**.
- Esse limite e fixo e nao configuravel.
- O objetivo e reduzir bloqueios do Zap Imoveis.
- Para varrer listagens longas, use chamadas sucessivas com `start_page`.

## Requisitos

- Python 3.10+ (recomendado)
- Playwright + Chromium

## Instalacao

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Uso via CLI

Exemplo completo:

```powershell
python .\find_zap_by_address.py `
  --url "https://www.zapimoveis.com.br/venda/apartamentos/rj+rio-de-janeiro/avenida-lucio-costa/?transacao=venda&onde=%2CRio+de+Janeiro%2CRio+de+Janeiro%2CZona+Oeste%2CBarra+da+Tijuca%2CAvenida+L%C3%BAcio+Costa%2C%2Cstreet%2CBR%3ERio+de+Janeiro%3ENULL%3ERio+de+Janeiro%3EZona+Oeste%3EBarra+da+Tijuca%2C-23.011213%2C-43.372959%2C&tipos=apartamento_residencial" `
  --address "Avenida Lucio Costa, 3604 - Barra da Tijuca, Rio de Janeiro - RJ" `
  --condominium "Barra Summer Dream" `
  --headless true `
  --recent-days 2 `
  --timeout 30000
```

Exemplo buscando so por condominio:

```powershell
python .\find_zap_by_address.py `
  --url "https://www.zapimoveis.com.br/venda/apartamentos/rj+rio-de-janeiro/avenida-lucio-costa/?transacao=venda&onde=%2CRio+de+Janeiro%2CRio+de+Janeiro%2CZona+Oeste%2CBarra+da+Tijuca%2CAvenida+L%C3%BAcio+Costa%2C%2Cstreet%2CBR%3ERio+de+Janeiro%3ENULL%3ERio+de+Janeiro%3EZona+Oeste%3EBarra+da+Tijuca%2C-23.011213%2C-43.372959%2C&tipos=apartamento_residencial" `
  --address "" `
  --condominium "Barra Summer Dream"
```

### Argumentos CLI

- `--url`: URL da listagem.
- `--address`: endereco parcial para match por trecho (contains apos normalizacao).
- `--condominium`: busca por nome do condominio na descricao (com heuristica anti-lista).
- `--headless`: `true` ou `false`.
- `--recent-days`: filtra anuncios criados nos ultimos N dias (0..30).
- `--timeout`: timeout em ms.
- `--start-page`: pagina inicial do lote atual.
  - O script ainda processa no maximo 2 paginas a partir dela.
  - Exemplos: `--start-page 1`, `--start-page 3`, `--start-page 5`.

### Comportamento

- Limite fixo: cada execucao processa no maximo 2 paginas (nao configuravel).
- Para varrer listagens longas, use chamadas sucessivas com `start_page`.
- `recent_days` vazio: match por endereco parcial e/ou condominio.
- `recent_days` preenchido com endereco e condominio vazios: retorna anuncios dentro da janela (sem filtro de texto).
- O match entre endereco e condominio usa modo `OU`.
- Validacao: e obrigatorio informar pelo menos um criterio (`--address`, `--condominium` ou `--recent-days`).

## Uso da API Flask

Suba o servidor:

```powershell
python .\web_app.py --host 127.0.0.1 --port 5000 --debug false
```

Endpoint principal:
- `POST /api/search`

O frontend local pode ser acessado em `http://127.0.0.1:5000` (rota `/`).

### Entradas aceitas

- `listing_url` (preferencial)
- `url` (fallback legado)
- `address`
- `condominium`
- `headless`
- `timeout`
- `recent_days`
- `start_page`

Observacoes:
- `listing_url` e o nome preferencial do campo.
- `url` continua aceito por compatibilidade.
- `max_pages` e ignorado mesmo que enviado.
- Erros de validacao retornam HTTP 400 com o mesmo body estruturado, usando `status="error"` e `stop_reason="validation_error"`.
- Em erros de validacao, `next_start_page` e `null` porque nao ha continuacao valida.

### Exemplo de requisicao

```json
{
  "listing_url": "https://www.zapimoveis.com.br/venda/apartamentos/...",
  "address": "Avenida Lucio Costa, 3604",
  "condominium": "Barra Summer Dream",
  "headless": "false",
  "timeout": 30000,
  "recent_days": 2,
  "start_page": 1
}
```

## Estrutura principal da resposta

Campos principais retornados pelo backend:
- `status`
- `start_page`
- `fixed_max_pages`
- `pages_processed`
- `last_page_processed`
- `next_start_page`
- `max_page_hint`
- `has_more_pages`
- `stop_reason`
- `total_matches`
- `matches`
- `visited_properties`
- `elapsed_seconds`
- `logs`

### Campos legados (compatibilidade)

Os campos abaixo permanecem temporariamente para manter compatibilidade com o frontend atual:
- `url`
- `urls`
- `matches_count`
- `pages_scanned`
- `matches_detail`
- `error`

Novas integracoes devem priorizar os campos principais acima.

## Exemplos de resposta JSON

Sem match, com mais paginas:

```json
{
  "status": "nao_encontrado",
  "start_page": 1,
  "fixed_max_pages": 2,
  "pages_processed": 2,
  "last_page_processed": 2,
  "next_start_page": 3,
  "max_page_hint": 5,
  "has_more_pages": true,
  "stop_reason": "fixed_limit_reached",
  "total_matches": 0,
  "matches": [],
  "visited_properties": 55,
  "elapsed_seconds": 182.6,
  "logs": [
    "[api] start_page=1 fixed_max_pages=2",
    "[fim] Limite fixo de paginas=2 atingido."
  ]
}
```

Com match:

```json
{
  "status": "encontrado",
  "start_page": 5,
  "fixed_max_pages": 2,
  "pages_processed": 1,
  "last_page_processed": 5,
  "next_start_page": null,
  "max_page_hint": 5,
  "has_more_pages": false,
  "stop_reason": "reached_last_page",
  "total_matches": 1,
  "matches": [
    {
      "listing_id": "123456789",
      "property_url": "https://www.zapimoveis.com.br/imovel/...",
      "address_extracted": "Avenida Lucio Costa, 3602 - Barra da Tijuca, Rio de Janeiro - RJ",
      "address_match": true,
      "condominium_match": true
    }
  ],
  "visited_properties": 22,
  "elapsed_seconds": 61.4,
  "logs": [
    "[batch] last_page_processed=5",
    "[batch] has_more_pages=false"
  ]
}
```

Erro operacional controlado:

```json
{
  "status": "error",
  "start_page": 3,
  "fixed_max_pages": 2,
  "pages_processed": 0,
  "last_page_processed": 2,
  "next_start_page": 3,
  "max_page_hint": 5,
  "has_more_pages": true,
  "stop_reason": "blocked_or_failed",
  "total_matches": 0,
  "matches": [],
  "visited_properties": 0,
  "elapsed_seconds": 12.1,
  "error_message": "Timeout opening page 3",
  "logs": [
    "[erro] Timeout opening page 3"
  ]
}
```

## Orquestracao em lotes (frontend ou n8n)

Cada chamada processa no maximo 2 paginas. Para percorrer listagens longas, orquestre chamadas sucessivas com `start_page`:

- Chamada 1: `start_page=1` -> processa paginas 1 e 2
- Chamada 2: `start_page=3` -> processa paginas 3 e 4
- Chamada 3: `start_page=5` -> processa pagina 5 (se for a ultima)
- Pare quando `has_more_pages=false`

Os campos `next_start_page` e `has_more_pages` existem exatamente para esse fluxo.

### Loop direto no frontend (sem n8n)

O frontend (`/`) agora pode rodar **em lotes ate o fim** sem usar n8n. Basta escolher:
- **Modo de execucao: Rodar em lotes ate o fim**

O frontend vai:
- chamar o backend com `start_page`
- esperar o retorno
- continuar com `next_start_page` ate `has_more_pages=false`
- respeitar pausas aleatorias entre lotes (45–90s)

### Workflow n8n (opcional)

O workflow corrigido esta em:
- `workflow_n8n.json` (principal)
- `Json teste.json` (copia)

Importe um deles no n8n e use sem edicoes no node **HTTP Request - Buscar Lote**.
Esse node foi configurado para enviar o body como **objeto JSON via expressao**, evitando o erro
`JSON parameter needs to be valid JSON`.

### Payload do Webhook (exemplo)

Envie JSON puro (sem `=` no inicio dos valores). Exemplo PowerShell:

```powershell
$body = @{
  listing_url = "https://www.zapimoveis.com.br/venda/apartamentos/rj+rio-de-janeiro/avenida-lucio-costa/?onde=%2CRio+de+Janeiro%2CRio+de+Janeiro%2CZona+Oeste%2CBarra+da+Tijuca%2CAvenida+L%C3%BAcio+Costa%2C%2Cstreet%2CBR%3ERio+de+Janeiro%3ENULL%3ERio+de+Janeiro%3EZona+Oeste%3EBarra+da+Tijuca%2C-23.011213%2C-43.372959%2C&tipos=apartamento_residencial&precoMaximo=9000000&precoMinimo=2500000&ordem=MOST_RECENT"
  address = "Avenida Lucio Costa, 3602"
  condominium = "Costa Del Sol"
  headless = $false
  timeout = 60000
  recent_days = 7
  start_page = 1
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri "https://n8n.seu-dominio.com/webhook/SEU_ID" `
  -Method POST `
  -ContentType "application/json" `
  -Body $body
```

Se algum valor chegar com `=` no inicio (ex.: `=https://...`), o backend agora remove esse prefixo automaticamente.

## Deduplicacao de resultados

Os matches retornados no lote sao deduplicados assim:
- primeiro por `listing_id`
- se nao houver `listing_id`, por `property_url`

O `listing_id` e extraido do URL quando houver padrao `-id-<numero>`.

## Compatibilidade com frontend legado

O backend continua devolvendo campos antigos (`url`, `urls`, `matches_count`, `pages_scanned`, `matches_detail`, `error`) para evitar quebra do frontend atual.
Novas integracoes devem usar o contrato principal documentado em **Estrutura principal da resposta**.

## Observacoes e troubleshooting

- O limite fixo em 2 paginas existe para reduzir bloqueios.
- Use `headless=false` quando possivel.
- Aumente `timeout` para `45000` ou `60000` em conexoes mais lentas.
- Evite varias buscas em paralelo.

## Ajuste de seletores (se o HTML do Zap mudar)

Edite em `find_zap_by_address.py`:
- `LISTING_CARD_SELECTORS`
- `LISTING_LINK_SELECTOR`
- `NEXT_PAGE_SELECTORS`
- `LOCATION_KEYWORDS`
- `ADDRESS_HINTS`
