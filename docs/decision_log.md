
## ARCHITECTURE_V2_FINANCIAL_DOMAINS

Decisão:
- Separar Raw/Landing de Bronze.
- Bronze passa a representar extração estruturada fiel ao source.
- Silver será normalizada em 3FN.
- Gold será modelo analítico canônico em fatos e dimensões.
- Marts serão especializações financeiras: FP&A, controladoria, tesouraria, contas a pagar, inadimplência, fornecedores, auditoria, governança documental, risco, procurement, contratos, jurídico-financeiro, ESG e benchmarking.

Motivo:
- Evitar confusão entre arquivo bruto e dado extraído.
- Preservar rastreabilidade.
- Permitir conciliação documental.
- Preparar especializações financeiras sem refazer o projeto.
