# ANP Câmbio — Inteligência Comercial

Painel de inteligência comercial para prospecção de clientes de câmbio com base no Relatório de Desembaraços de Importações da ANP.

## Como funciona

- Acessa diariamente o relatório público da ANP
- Processa os dados de importações de petróleo, gás, derivados e biocombustíveis
- Exibe ranking de importadores, volumes, FX estimado e contatos para prospecção

## Deploy no GitHub Pages

1. Suba este repositório no GitHub
2. Vá em **Settings → Pages → Source: Deploy from branch → main → / (root)**
3. Aguarde ~2 minutos — o app estará disponível em `https://SEU_USUARIO.github.io/anp-cambio`

## Atualização automática

O arquivo `.github/workflows/update_data.yml` configura o GitHub Actions para:
- Rodar todo dia às 06h horário de Brasília
- Baixar o Excel mais recente da ANP
- Processar os dados e fazer commit automático
- O app atualiza sozinho — sem você precisar fazer nada

## Estrutura

```
├── index.html              # App principal
├── data/
│   ├── meta.json           # Metadados da última atualização
│   ├── records_2026.json   # Dados processados 2026
│   └── records_2025.json   # Dados processados 2025
├── src/
│   └── fetch_anp.py        # Script de coleta e processamento
├── .github/workflows/
│   └── update_data.yml     # Agendamento automático
└── requirements.txt
```

## Fonte dos dados

[ANP — Relatório de Desembaraços](https://www.gov.br/anp/pt-br/assuntos/importacoes-e-exportacoes/relatorio-de-desembaracos-de-importacoes-de-petroleo-gas-derivados-e-biocombustiveis)  
