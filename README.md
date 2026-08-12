# 💊 MEX PHARMA — Plataforma End-to-End de Datos e Inteligencia Artificial
## 👤 Autor

**Manuel Medina**

Data Engineer | Databricks | Azure | Data & AI

Proyecto desarrollado como caso práctico end-to-end de ingeniería de datos, analítica e Inteligencia Artificial.

![Status](https://img.shields.io/badge/Estado-POC_Completado-success)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)
![Azure](https://img.shields.io/badge/Microsoft_Azure-Cloud-blue)
![Power BI](https://img.shields.io/badge/Power_BI-Analytics-yellow)
![AI](https://img.shields.io/badge/IA-LangChain-purple)

## 📌 Descripción del proyecto

**MEX PHARMA** es una prueba de concepto (POC) de una plataforma de datos e inteligencia artificial para el análisis de información farmacéutica, ventas e inventario.

El proyecto demuestra la construcción de una solución **end-to-end**, comenzando con la ingesta de información desde una API pública, continuando con su almacenamiento, procesamiento, validación y modelado mediante una arquitectura Lakehouse, y terminando con dos productos de consumo:

- Un dashboard ejecutivo desarrollado en **Power BI**.
- Un asistente de IA que permite consultar información de ventas, inventario y riesgo de reabastecimiento utilizando lenguaje natural.

El objetivo no es únicamente visualizar datos, sino demostrar cómo diferentes componentes de una plataforma moderna de datos pueden integrarse para transformar información cruda en herramientas que apoyen la toma de decisiones.

---

## 🎯 Problema de negocio

Una compañía farmacéutica necesita integrar información de productos proveniente de fuentes externas y convertirla en información confiable que pueda utilizarse para analizar:

- desempeño de ventas;
- productos con mayor generación de ingresos;
- comportamiento por región;
- niveles actuales de inventario;
- demanda promedio;
- días disponibles de inventario;
- productos con riesgo de desabasto;
- prioridades de reabastecimiento.

Además de los dashboards tradicionales, se plantea permitir que un usuario de negocio pueda realizar preguntas directamente sobre los datos utilizando lenguaje natural.

Ejemplos:

> **¿Qué productos requieren reabastecimiento urgente?**

> **¿Cuáles son los productos con mayores ventas?**

> **¿Qué región genera mayores ingresos?**

> **¿Qué productos críticos existen en la región East?**

---

## 💡 Solución propuesta

Se diseñó una arquitectura de datos que integra servicios de **Microsoft Azure, Databricks, Power BI e Inteligencia Artificial**.

De manera simplificada:

```text
openFDA API
     │
     ▼
Azure Data Factory
     │
     ▼
Azure Data Lake Storage Gen2
     │
     ▼
Databricks Lakehouse
     │
     ├── Bronze
     │
     ├── Silver
     │
     └── Gold
          │
          ├──────────────► Power BI
          │
          └──────────────► AI Agent
                                │
                                ▼
                         Databricks App


