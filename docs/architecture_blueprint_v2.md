# Architecture Blueprint v2 — Condomínio Floriano Data

## Camadas

1. manual_upload/
   - entrada manual controlada;
   - PDFs originais da prestação;
   - comprovantes baixados;
   - links extraídos;
   - não versionado no Git.

2. landing/raw
   - preservação imutável;
   - hash;
   - run_id;
   - manifesto;
   - metadados de origem.

3. bronze
   - primeira extração estruturada fiel ao source;
   - demonstrativo;
   - inadimplência;
   - despesas;
   - links;
   - comprovantes resumidos;
   - documentos adicionais.

4. silver
   - normalização 3FN;
   - fornecedores;
   - competências;
   - contas;
   - categorias;
   - lançamentos;
   - documentos;
   - unidades;
   - acordos;
   - inadimplência.

5. gold
   - fatos e dimensões analíticas canônicas;
   - fato despesa;
   - fato receita;
   - fato saldo;
   - fato inadimplência;
   - fato conciliação documental;
   - dimensões de tempo, fornecedor, categoria, unidade, conta e documento.

6. marts
   - FP&A;
   - controladoria;
   - tesouraria;
   - contas a pagar;
   - inadimplência;
   - fornecedores;
   - auditoria financeira;
   - governança documental;
   - risco financeiro;
   - procurement;
   - contratos;
   - jurídico-financeiro;
   - ESG operacional;
   - benchmarking.

## Ordem operacional

PDF mensal em manual_upload/YYYY_MM/source
→ extração raw
→ bronze fiel
→ silver normalizada
→ download documental
→ classificação de comprovantes
→ conciliação
→ gold
→ marts.
