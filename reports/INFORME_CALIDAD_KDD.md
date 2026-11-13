# Informe de Calidad de Datos y Preparación KDD (ISO/IEC 25012)

## 1. Resumen Ejecutivo
Informe generado automáticamente tras la ejecución del pipeline de validación criptográfica y saneamiento sobre el lote transaccional.

## 2. Métricas de Calidad y Perfilado
* **Registros Procesados Iniciales**: 100
* **Registros Válidos Finales**: 92
* **Tasa de Completitud (ISO/IEC 25012)**: 92.0%
* **Registros en Cuarentena**: 8

### Distribución estadística de `importe_eur` (post-limpieza):
* **Media**: 158.87 €
* **Mediana**: 73.57 €
* **Desviación Estándar**: 523.22 €
* **Percentil 25 (Q1)**: 54.14 €
* **Percentil 75 (Q3)**: 87.69 €

## 3. Conclusiones Técnicas
Se han segregado correctamente los ficheros con alteraciones criptográficas y los registros con nulos o valores atípicos de sensores, asegurando que el dataset final cumpla con los estándares de calidad para los modelos de Business Intelligence.
