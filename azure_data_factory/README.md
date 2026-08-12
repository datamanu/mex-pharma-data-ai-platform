# Azure Data Factory

ADF se utiliza como capa de ingesta y orquestación.

## Flujo implementado

openFDA REST API  
> REST Linked Service  
> Copy Activity  
> ADLS Gen2  
> Bronze

## Componentes configurados

- Linked Service REST para openFDA
- Linked Service para ADLS Gen2
- Copy Activity
- Managed Identity para autenticación contra Storage
- Escritura del JSON crudo en la capa Bronze

## Objetivo arquitectónico

ADF se utiliza principalmente para orquestación e ingesta, mientras que las transformaciones pesadas se realizan en Databricks.

Esto permite separar responsabilidades entre:

- Ingesta
- Almacenamiento
- Procesamiento
- Consumo
