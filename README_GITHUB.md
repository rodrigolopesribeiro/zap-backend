# Deploy via GitHub (cópia segura)

Esta pasta é uma **cópia para deploy**, independente do projeto original.

## Conteúdo
- `web_app.py`, `find_zap_by_address.py`, `templates/` e `requirements.txt`.
- `start.sh` (Linux) e `start.ps1` (Windows).
- `.gitignore` com exclusão de segredos e arquivos locais.

## Como usar no VPS (resumo)
1. `git clone` do repositório.
2. `python -m venv .venv && source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `python -m playwright install chromium`
5. `python -m playwright install-deps`
6. `./start.sh`

## Observações
- O backend continua com o limite fixo de 2 páginas por execução.
- Não coloque chaves privadas/segredos no repositório.
