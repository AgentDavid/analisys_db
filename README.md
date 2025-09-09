# Generador de DBML para PostgreSQL

Este proyecto es un script en Node.js diseñado para generar automáticamente archivos DBML (Database Markup Language) a partir de una base de datos PostgreSQL. Utiliza metadatos de la base de datos para crear un esquema completo, incluyendo tablas, columnas, relaciones, índices y restricciones. Opcionalmente, integra inteligencia artificial (Gemini de Google) para enriquecer las descripciones de tablas y columnas con contexto semántico.

## Características Principales

- **Extracción Completa de Metadatos**: Obtiene esquemas, tablas, columnas, tipos de datos, claves primarias, foráneas, índices únicos y restricciones de verificación.
- **Relaciones Explícitas e Inferidas**: Detecta relaciones foráneas existentes y puede inferir nuevas basadas en nombres de columnas o coincidencias de valores en los datos.
- **Integración con IA**: Usa Gemini para generar descripciones automáticas de tablas y columnas, mejorando la documentación del esquema.
- **Concurrencia y Rendimiento**: Procesa tablas en paralelo para optimizar el tiempo de ejecución en bases de datos grandes.
- **Validación DBML**: Verifica la sintaxis del archivo generado para asegurar compatibilidad.
- **Manejo de Errores y Reintentos**: Incluye reintentos para llamadas a IA y manejo de permisos restringidos.
- **Configuración Flexible**: Variables de entorno para personalizar comportamiento, como tamaño de muestra, concurrencia, etc.
- **Reportes y Métricas**: Genera reportes de tablas sin acceso y métricas de procesamiento.

## Requisitos

- Node.js (versión 14 o superior)
- Acceso a una base de datos PostgreSQL
- (Opcional) Clave API de Google Gemini para usar IA

## Instalación

1. Clona o descarga el proyecto en tu directorio local.
2. Instala las dependencias:

   ```bash
   npm install
   ```

3. Configura las variables de entorno en un archivo `.env` o directamente en el entorno:
   - `DB_USER`: Usuario de la base de datos
   - `DB_HOST`: Host de la base de datos
   - `DB_DATABASE`: Nombre de la base de datos
   - `DB_PASSWORD`: Contraseña
   - `DB_PORT`: Puerto (por defecto 5432)
   - `DB_SSL`: 'true' si usa SSL
   - `USE_AI`: 'true' para activar IA
   - `GEMINI_API_KEY`: Clave API de Gemini (si USE_AI=true)
   - Otras variables opcionales como `SAMPLE_SIZE`, `TABLE_CONCURRENCY`, etc.

## Uso

Ejecuta el script principal:

```bash
node generar-dbml.js
```

El script generará un archivo `database_completa.dbml` con el esquema completo. Si hay errores de validación, se guardará en `database_invalida.dbml`.

### Opciones de Configuración

- `OUTPUT_FILE`: Archivo de salida principal (por defecto: `database_completa.dbml`)
- `ERROR_OUTPUT_FILE`: Archivo para errores (por defecto: `database_invalida.dbml`)
- `PARTIAL_FILE`: Archivo de guardado parcial (por defecto: `database_parcial.dbml`)
- `SAMPLE_SIZE`: Número de filas de muestra por tabla (por defecto: 20)
- `TABLE_CONCURRENCY`: Número de tablas procesadas en paralelo (por defecto: 4)
- `USE_AI`: Activar/desactivar IA (por defecto: false)
- `INFER_FKS`: Inferir relaciones foráneas por datos (por defecto: true)
- Y muchas más en el código fuente.

## Logros del Proyecto

Este proyecto destaca por varios logros técnicos y funcionales:

1. **Automatización Completa**: Transforma una base de datos PostgreSQL en un archivo DBML válido sin intervención manual, ahorrando tiempo en documentación de esquemas.

2. **Integración Avanzada con IA**: Incorpora Gemini para generar descripciones contextuales, convirtiendo metadatos técnicos en documentación legible y útil para desarrolladores y analistas.

3. **Inferencia Inteligente de Relaciones**: No solo extrae FKs existentes, sino que infiere nuevas relaciones basadas en patrones de nombres y coincidencias de datos, mejorando la precisión del modelo relacional.

4. **Optimización de Rendimiento**: Usa concurrencia para procesar tablas en paralelo, reduciendo significativamente el tiempo de ejecución en bases de datos con muchas tablas.

5. **Robustez y Fiabilidad**: Maneja errores de permisos, reintentos en llamadas a IA, validación sintáctica y guardado parcial para evitar pérdida de progreso.

6. **Flexibilidad y Configurabilidad**: Más de 20 variables de entorno permiten adaptar el comportamiento a diferentes entornos y necesidades, desde desarrollo hasta producción.

7. **Métricas y Transparencia**: Proporciona métricas detalladas de procesamiento, incluyendo tablas procesadas, tiempo total y reportes de acceso restringido.

8. **Compatibilidad con DBML**: Genera archivos compatibles con herramientas como dbdiagram.io, facilitando la visualización y edición de esquemas.

9. **Escalabilidad**: Diseñado para manejar bases de datos grandes, con límites configurables y manejo eficiente de memoria.

10. **Código Limpio y Documentado**: El script está bien estructurado con comentarios en español, facilitando mantenimiento y extensiones futuras.

## Estructura del Proyecto

- `generar-dbml.js`: Script principal para PostgreSQL.
- `gen-dbml-mysql.js`: Versión alternativa para MySQL (no detallada en este README).
- `package.json`: Dependencias y configuración de Node.js.

## Contribución

Si deseas contribuir, por favor:

1. Haz un fork del proyecto.
2. Crea una rama para tu feature.
3. Envía un pull request con descripciones detalladas.

## Licencia

Este proyecto es de código abierto. Consulta el archivo LICENSE para más detalles.

## Soporte

Para preguntas o issues, abre un issue en el repositorio o contacta al maintainer.
