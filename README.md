# ShopStream BigData

Pipeline de datos en tiempo real para e-commerce usando AWS (Kinesis, Lambda, Spark, Glue).

## Estructura del Proyecto

```
shopstream-bigdata/
|
+-- README.md
+-- requirements.txt
+-- .gitignore
|
+-- data-generator/      # Generador de datos simulados
+-- lambda-validator/    # Funciones Lambda para validacion
+-- spark-jobs/          # Jobs de procesamiento con Spark
+-- api/                 # API REST
+-- tests/               # Tests del proyecto
+-- notebooks/           # Notebooks de analisis
+-- glue/                # Jobs de AWS Glue
+-- infrastructure/      # IaC (Terraform / CloudFormation)
+-- .github/workflows/   # CI/CD pipelines
```
