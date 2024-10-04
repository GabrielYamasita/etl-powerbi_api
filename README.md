# ETL Usando Dataset do Power BI como Fonte

Neste projeto usei um Semantic Model do Power BI como fonte de dados, realizando um ETL totalmente em Python para extração dos dados, transformação e carga em um banco de dados.

Os dados são extraídos com uso de consultas DAX que são executas via REST API do Power BI, que utiliza autenticação definida na Azure AD.

Como é uma REST API os dados retornados pelo DAX estão em formato JSON, e assim são gravados em uma pasta servindo como staging.

Logo então os dados da pasta são lidos, tratados, formatados em JSON novamente para inserção no banco de dados Supabase.
