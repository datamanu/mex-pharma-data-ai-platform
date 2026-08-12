# Azure Data Lake Storage Gen2

ADLS Gen2 funciona como la capa de almacenamiento cloud de la solución.

## Estructura utilizada

bronze/
 openfda/
     drug_labels.json

pharma/
 managed storage para Unity Catalog

## Diseño

La capa `bronze` conserva los archivos raw provenientes de la fuente externa.

El contenedor `pharma` se utiliza para almacenamiento administrado asociado con Unity Catalog.

## Seguridad

El acceso se realiza mediante Managed Identity y permisos RBAC.

No se almacenan credenciales de Storage directamente en los notebooks.
