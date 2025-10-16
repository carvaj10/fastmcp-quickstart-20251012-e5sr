import os
import json
import pyodbc
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Cargar variables de entorno
load_dotenv()

# Configuración de las bases de datos
DB_CONFIGS = {
    "default": {
        "server": os.getenv("DEV_SERVER"),
        "database": os.getenv("DEV_DATABASE"),
        "username": os.getenv("DEV_USERNAME"),
        "password": os.getenv("DEV_PASSWORD"),
        "driver": "{ODBC Driver 17 for SQL Server}",
    },
    "INTEGRACION_CW_20_DEV": {
        "server": os.getenv("DEV_SERVER"),
        "database": "INTEGRACION_CW_20_DEV",
        "username": os.getenv("DEV_USERNAME"),
        "password": os.getenv("DEV_PASSWORD"),
        "driver": "{ODBC Driver 17 for SQL Server}",
    },
}

# Inicializar FastMCP
mcp = FastMCP("Ecosystem-Report-Generator")


def get_db_connection(database_key: str = "default"):
    """
    Crea y retorna una conexión a la base de datos especificada

    Args:
        database_key: Clave de la base de datos ('default' o 'INTEGRACION_CW_20_DEV')

    Returns:
        Conexión a la base de datos
    """
    if database_key not in DB_CONFIGS:
        raise ValueError(f"Base de datos '{database_key}' no configurada")

    config = DB_CONFIGS[database_key]
    conn_string = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']}"
    )
    return pyodbc.connect(conn_string)


@mcp.tool()
def get_table_structure(table_name: str, database_key: str = "default") -> str:
    """
    Obtiene la estructura de una tabla específica en la base de datos especificada

    Args:
        table_name: Nombre de la tabla a consultar
        database_key: Base de datos a consultar ('default' o 'INTEGRACION_CW_20_DEV')

    Returns:
        Estructura de la tabla con columnas y tipos de datos
    """
    try:
        conn = get_db_connection(database_key)
        cursor = conn.cursor()

        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """

        cursor.execute(query, table_name)
        columns = cursor.fetchall()

        if not columns:
            return f"⚠️ No se encontró la tabla '{table_name}' en la base de datos '{DB_CONFIGS[database_key]['database']}'"

        result = f"📋 Estructura de la tabla: {table_name}\n"
        result += f"🗄️ Base de datos: {DB_CONFIGS[database_key]['database']}\n\n"
        for col in columns:
            col_name, data_type, max_length, nullable = col
            length_info = f"({max_length})" if max_length else ""
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            result += f"  • {col_name}: {data_type}{length_info} - {null_info}\n"

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al obtener estructura: {str(e)}"


@mcp.tool()
def list_tables(schema: str = "dbo", database_key: str = "default") -> str:
    """
    Lista todas las tablas disponibles en un esquema de la base de datos especificada

    Args:
        schema: Nombre del esquema (por defecto 'dbo')
        database_key: Base de datos a consultar ('default' o 'INTEGRACION_CW_20_DEV')

    Returns:
        Lista de tablas disponibles
    """
    try:
        conn = get_db_connection(database_key)
        cursor = conn.cursor()

        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """

        cursor.execute(query, schema)
        tables = cursor.fetchall()

        result = f"📚 Tablas disponibles en el esquema '{schema}'\n"
        result += f"🗄️ Base de datos: {DB_CONFIGS[database_key]['database']}\n\n"
        for table in tables:
            result += f"  • {table[0]}\n"

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al listar tablas: {str(e)}"


@mcp.tool()
def test_query(query: str, business_unit: str, database_key: str = "default") -> str:
    """
    Prueba una query antes de crear el reporte

    Args:
        query: Query SQL a probar (debe incluir @business_unit)
        business_unit: Valor del business_unit para probar
        database_key: Base de datos a consultar ('default' o 'INTEGRACION_CW_20_DEV')

    Returns:
        Resultados de la query de prueba
    """
    try:
        if "@business_unit" not in query:
            return "⚠️ La query debe incluir el parámetro @business_unit"

        conn = get_db_connection(database_key)
        cursor = conn.cursor()

        # Reemplazar el parámetro para la prueba
        test_query = query.replace("@business_unit", f"'{business_unit}'")

        cursor.execute(test_query)
        rows = cursor.fetchall()

        if not rows:
            return f"✅ Query ejecutada correctamente en {DB_CONFIGS[database_key]['database']} pero no retornó resultados"

        # Obtener nombres de columnas
        columns = [column[0] for column in cursor.description]

        result = f"✅ Query ejecutada correctamente en {DB_CONFIGS[database_key]['database']}. Primeras 5 filas:"
        result += " | ".join(columns) + "\n"
        result += "-" * 80 + "\n"

        for row in rows[:5]:
            result += " | ".join(str(val) for val in row) + "\n"

        result += f"\n📊 Total de registros: {len(rows)}"

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al probar query: {str(e)}"


@mcp.tool()
def create_report(
    report_prefix: str,
    report_description_en: str,
    report_description_es: str,
    query: str,
    additional_params: dict = None,
    is_detail: int = 0,
    has_detail: int = None,
    action_column: str = None,
    detail_prefix: str = None,
    detail_mode: str = None,
    open_another_tab: int = None,
    type_resource: str = "table",
    columns_to_render: str = None,
    default_for_all: int = 0,
    database_key: str = None,
) -> str:
    """
    Crea un nuevo reporte ejecutando el SP sp_ecosystem_create_columns_config

    Args:
        report_prefix: Prefijo único del reporte
        report_description_en: Descripción en inglés
        report_description_es: Descripción en español
        query: Query SQL (debe incluir @business_unit)
        additional_params: Parámetros adicionales además de business_unit (dict)
        is_detail: Si es un detalle (0 o 1)
        has_detail: Si tiene detalle (NULL, 0, 1)
        action_column: Columna de acción
        detail_prefix: Prefijo del detalle
        detail_mode: Modo del detalle ('modal', 'page')
        open_another_tab: Abrir en otra pestaña (0, 1)
        type_resource: Tipo de recurso (por defecto 'table')
        columns_to_render: Columnas a renderizar (NULL para todas)
        default_for_all: Reporte por defecto para todas las empresas (0, 1)
        database_key: Base de datos específica para la query ('default' o 'INTEGRACION_CW_20_DEV'). Si es None, se detecta automáticamente. NOTA: El SP siempre se ejecuta en la base de datos por defecto.

    Returns:
        Resultado de la ejecución del SP
    """
    try:
        if "@business_unit" not in query:
            return "⚠️ La query debe incluir el parámetro @business_unit"

        # Detectar automáticamente la base de datos para la query si no se especifica
        query_database_key = None
        if database_key is None:
            # Si la query menciona INTEGRACION_CW_20_DEV, la query apunta a esa base de datos
            if "INTEGRACION_CW_20_DEV" in query.upper():
                query_database_key = "INTEGRACION_CW_20_DEV"
            else:
                query_database_key = "default"
        else:
            query_database_key = database_key

        # Construir params_config
        params_config = {"business_unit": "NVARCHAR(20)"}
        if additional_params:
            params_config.update(additional_params)

        params_json = json.dumps(params_config)

        # El SP siempre se ejecuta en la base de datos por defecto (dev)
        conn = get_db_connection("default")
        cursor = conn.cursor()

        sp_call = """
        EXEC sp_ecosystem_create_columns_config
            ?, -- @report_prefix
            ?, -- @report_description
            ?, -- @report_description_es
            'report', -- @type
            1, -- @table_number
            ?, -- @query
            ?, -- @params_config
            ?, -- @is_detail
            ?, -- @hasDetail
            ?, -- @actionColumn
            ?, -- @detail_prefix
            ?, -- @detail_mode
            ?, -- @open_another_tab
            ?, -- @type_resource
            ?, -- @columns_to_render
            ?  -- default_for_all
        """

        cursor.execute(
            sp_call,
            (
                report_prefix,
                report_description_en,
                report_description_es,
                query,
                params_json,
                is_detail,
                has_detail,
                action_column,
                detail_prefix,
                detail_mode,
                open_another_tab,
                type_resource,
                columns_to_render,
                default_for_all,
            ),
        )

        conn.commit()
        conn.close()

        return f"""✅ Reporte creado exitosamente!

📄 Detalles del reporte:
  • Prefijo: {report_prefix}
  • Descripción (EN): {report_description_en}
  • Descripción (ES): {report_description_es}
  • Parámetros: {params_json}
  • Tipo de recurso: {type_resource}
  • Query apunta a: {DB_CONFIGS[query_database_key]["database"]}
  • SP ejecutado en: {DB_CONFIGS["default"]["database"]}
"""

    except Exception as e:
        return f"❌ Error al crear el reporte: {str(e)}"


@mcp.tool()
def assign_report_to_role(
    report_prefix: str,
    business_unit: str,
    role_description: str,
    application_type: str = None,
    order: int = None,
    custom_tag: str = None,
    sales_office: str = None,
    center_logistical: str = None,
) -> str:
    """
    Asigna un reporte a un rol específico en la tabla assigned_reports

    Args:
        report_prefix: Prefijo del reporte a asignar
        business_unit: Unidad de negocio
        role_description: Descripción del rol (ej: "administrador") - el sistema buscará el código
        application_type: Tipo de aplicación ('sales_force' o 'merchandising')
        order: Orden de visualización (opcional)
        custom_tag: Etiqueta personalizada (opcional)
        sales_office: Oficina de ventas (opcional)
        center_logistical: Centro logístico (opcional)

    Returns:
        Resultado de la asignación
    """
    try:
        conn = get_db_connection("default")
        cursor = conn.cursor()

        # Paso 1: Buscar el código del rol por descripción
        role_query = """
        SELECT [code], [description], [application_type]
        FROM default_roles 
        WHERE business_unit = ? AND [description] LIKE ? AND application_type != 'sys_admin'
        """

        cursor.execute(role_query, (business_unit, f"%{role_description}%"))
        roles = cursor.fetchall()

        if not roles:
            return f"❌ No se encontró ningún rol con descripción '{role_description}' para la unidad {business_unit} (excluyendo sys_admin)"

        # Si hay múltiples roles, usar el primero
        role_code, role_desc, role_app_type = roles[0]

        warning_msg = ""
        if len(roles) > 1:
            warning_msg = f"⚠️ Se encontraron múltiples roles. Usando: {role_code} - {role_desc}\n\n"

        # Paso 2: Verificar si ya existe la asignación
        check_query = """
        SELECT COUNT(*) FROM assigned_reports 
        WHERE business_unit = ? AND report_prefix = ? AND [role] = ?
        """

        cursor.execute(check_query, (business_unit, report_prefix, role_code))
        exists = cursor.fetchone()[0]

        if exists > 0:
            return f"⚠️ El reporte '{report_prefix}' ya está asignado al rol '{role_code}' para la unidad {business_unit}"

        # Paso 3: Insertar la nueva asignación
        insert_query = """
        INSERT INTO assigned_reports (
            [role], report_prefix, business_unit, application_type, [order], 
            custom_tag, sales_office, center_logistical
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(
            insert_query,
            (
                role_code,
                report_prefix,
                business_unit,
                application_type,
                order,
                custom_tag,
                sales_office,
                center_logistical,
            ),
        )

        conn.commit()
        conn.close()

        return f"""{warning_msg}✅ Reporte asignado exitosamente!

📋 Detalles de la asignación:
  • Reporte: {report_prefix}
  • Unidad: {business_unit}
  • Rol: {role_code} - {role_desc}
  • Tipo de aplicación: {application_type or "N/A"}
  • Orden: {order or "N/A"}
  • Etiqueta personalizada: {custom_tag or "N/A"}
  • Oficina de ventas: {sales_office or "N/A"}
  • Centro logístico: {center_logistical or "N/A"}
"""

    except Exception as e:
        return f"❌ Error al asignar reporte: {str(e)}"


@mcp.tool()
def get_table_structures_across_databases(table_names: list) -> str:
    """
    Obtiene la estructura de múltiples tablas en todas las bases de datos configuradas

    Args:
        table_names: Lista de nombres de tablas a consultar

    Returns:
        Estructura de las tablas en todas las bases de datos
    """
    try:
        if not table_names:
            return "⚠️ No se proporcionaron nombres de tablas"

        result = "🔍 Buscando estructuras de tablas en todas las bases de datos:\n"
        result += f"📋 Tablas solicitadas: {', '.join(table_names)}\n\n"

        for db_key, db_config in DB_CONFIGS.items():
            try:
                conn = get_db_connection(db_key)
                cursor = conn.cursor()

                # Crear placeholders para la consulta IN
                placeholders = ",".join(["?" for _ in table_names])

                query = f"""
                SELECT 
                    TABLE_NAME,
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME IN ({placeholders})
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """

                cursor.execute(query, table_names)
                columns = cursor.fetchall()

                result += f"🗄️ Base de datos: {db_config['database']}\n"

                if columns:
                    # Agrupar resultados por tabla
                    tables_data = {}
                    for col in columns:
                        (
                            table_name,
                            col_name,
                            data_type,
                            max_length,
                            nullable,
                            ordinal,
                        ) = col
                        if table_name not in tables_data:
                            tables_data[table_name] = []

                        length_info = f"({max_length})" if max_length else ""
                        null_info = "NULL" if nullable == "YES" else "NOT NULL"
                        tables_data[table_name].append(
                            {
                                "name": col_name,
                                "type": data_type,
                                "length": length_info,
                                "nullable": null_info,
                                "position": ordinal,
                            }
                        )

                    # Mostrar tablas encontradas
                    for table_name in table_names:
                        if table_name in tables_data:
                            result += f"  ✅ {table_name}:\n"
                            for col in tables_data[table_name]:
                                result += f"    • {col['name']}: {col['type']}{col['length']} - {col['nullable']}\n"
                        else:
                            result += f"  ❌ {table_name}: No encontrada\n"
                else:
                    result += "  ❌ Ninguna tabla encontrada\n"

                result += "\n"
                conn.close()

            except Exception as e:
                result += f"⚠️ Error al consultar {db_config['database']}: {str(e)}\n\n"

        return result

    except Exception as e:
        return f"❌ Error general: {str(e)}"


@mcp.tool()
def get_multiple_table_structures(
    table_names: list, database_key: str = "default"
) -> str:
    """
    Obtiene la estructura de múltiples tablas de una vez

    Args:
        table_names: Lista de nombres de tablas a consultar
        database_key: Base de datos a consultar ('default' o 'INTEGRACION_CW_20_DEV')

    Returns:
        Estructura de todas las tablas solicitadas
    """
    try:
        if not table_names:
            return "⚠️ No se proporcionaron nombres de tablas"

        conn = get_db_connection(database_key)
        cursor = conn.cursor()

        # Crear placeholders para la consulta IN
        placeholders = ",".join(["?" for _ in table_names])

        query = f"""
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME IN ({placeholders})
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """

        cursor.execute(query, table_names)
        columns = cursor.fetchall()

        if not columns:
            tables_str = ", ".join(table_names)
            return f"⚠️ No se encontraron las tablas '{tables_str}' en la base de datos '{DB_CONFIGS[database_key]['database']}'"

        # Agrupar resultados por tabla
        tables_data = {}
        for col in columns:
            table_name, col_name, data_type, max_length, nullable, ordinal = col
            if table_name not in tables_data:
                tables_data[table_name] = []

            length_info = f"({max_length})" if max_length else ""
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            tables_data[table_name].append(
                {
                    "name": col_name,
                    "type": data_type,
                    "length": length_info,
                    "nullable": null_info,
                    "position": ordinal,
                }
            )

        # Construir resultado
        result = (
            f"📋 Estructuras de tablas en {DB_CONFIGS[database_key]['database']}:\n\n"
        )

        for table_name in table_names:
            if table_name in tables_data:
                result += f"🗂️ Tabla: {table_name}\n"
                for col in tables_data[table_name]:
                    result += f"  • {col['name']}: {col['type']}{col['length']} - {col['nullable']}\n"
                result += "\n"
            else:
                result += f"❌ Tabla '{table_name}' no encontrada\n\n"

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al obtener estructuras múltiples: {str(e)}"


@mcp.tool()
def bulk_search_tables_in_databases(
    table_names: list, database_keys: list = None
) -> str:
    """
    Busca múltiples tablas en múltiples bases de datos de manera masiva

    Args:
        table_names: Lista de nombres de tablas a buscar
        database_keys: Lista de claves de bases de datos ('default', 'INTEGRACION_CW_20_DEV').
                      Si es None, busca en todas las bases de datos configuradas

    Returns:
        Resultado de la búsqueda masiva de tablas
    """
    try:
        if not table_names:
            return "⚠️ No se proporcionaron nombres de tablas para buscar"

        # Si no se especifican bases de datos, usar todas las configuradas
        if database_keys is None:
            database_keys = list(DB_CONFIGS.keys())

        # Validar que las bases de datos existan
        invalid_dbs = [db for db in database_keys if db not in DB_CONFIGS]
        if invalid_dbs:
            return f"❌ Bases de datos no válidas: {', '.join(invalid_dbs)}. Disponibles: {', '.join(DB_CONFIGS.keys())}"

        # Crear placeholders para la consulta IN
        placeholders = ",".join(["?" for _ in table_names])

        # Buscar en cada base de datos
        all_results = {}
        total_found = 0

        for db_key in database_keys:
            try:
                # Conectar a la base de datos específica
                db_conn = get_db_connection(db_key)
                db_cursor = db_conn.cursor()

                query = f"""
                SELECT 
                    TABLE_NAME,
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME IN ({placeholders})
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """

                db_cursor.execute(query, table_names)
                columns = db_cursor.fetchall()

                # Procesar resultados para esta base de datos
                db_results = {}
                found_tables = set()

                for col in columns:
                    (
                        table_name,
                        col_name,
                        data_type,
                        max_length,
                        nullable,
                        ordinal,
                    ) = col

                    if table_name not in db_results:
                        db_results[table_name] = []
                        found_tables.add(table_name)

                    length_info = f"({max_length})" if max_length else ""
                    null_info = "NULL" if nullable == "YES" else "NOT NULL"
                    db_results[table_name].append(
                        {
                            "name": col_name,
                            "type": data_type,
                            "length": length_info,
                            "nullable": null_info,
                            "position": ordinal,
                        }
                    )

                all_results[db_key] = {
                    "found_tables": db_results,
                    "found_count": len(found_tables),
                    "total_columns": len(columns),
                }
                total_found += len(found_tables)

                db_conn.close()

            except Exception as e:
                all_results[db_key] = {
                    "error": str(e),
                    "found_tables": {},
                    "found_count": 0,
                    "total_columns": 0,
                }

        # Construir resultado
        result = "🔍 Búsqueda masiva de tablas completada!\n\n"
        result += f"📋 Tablas buscadas: {', '.join(table_names)}\n"
        result += f"🗄️ Bases de datos consultadas: {', '.join(database_keys)}\n"
        result += f"📊 Total de tablas encontradas: {total_found}\n\n"

        # Mostrar resultados por base de datos
        for db_key in database_keys:
            db_config = DB_CONFIGS[db_key]
            db_result = all_results[db_key]

            result += f"🗄️ Base de datos: {db_config['database']}\n"

            if "error" in db_result:
                result += f"  ❌ Error: {db_result['error']}\n\n"
                continue

            if db_result["found_count"] == 0:
                result += "  ❌ Ninguna tabla encontrada\n\n"
                continue

            result += f"  ✅ Tablas encontradas: {db_result['found_count']} | Columnas totales: {db_result['total_columns']}\n"

            # Mostrar cada tabla encontrada
            for table_name in table_names:
                if table_name in db_result["found_tables"]:
                    columns = db_result["found_tables"][table_name]
                    result += f"    📋 {table_name} ({len(columns)} columnas):\n"

                    for col in columns[:5]:  # Mostrar solo las primeras 5 columnas
                        result += f"      • {col['name']}: {col['type']}{col['length']} - {col['nullable']}\n"

                    if len(columns) > 5:
                        result += f"      ... y {len(columns) - 5} columnas más\n"
                else:
                    result += f"    ❌ {table_name}: No encontrada\n"

            result += "\n"

        # Resumen final
        result += "📈 Resumen por tabla:\n"
        for table_name in table_names:
            found_in_dbs = []
            for db_key in database_keys:
                if (
                    db_key in all_results
                    and table_name in all_results[db_key]["found_tables"]
                ):
                    found_in_dbs.append(DB_CONFIGS[db_key]["database"])

            if found_in_dbs:
                result += (
                    f"  ✅ {table_name}: Encontrada en {', '.join(found_in_dbs)}\n"
                )
            else:
                result += f"  ❌ {table_name}: No encontrada en ninguna base de datos\n"

        return result

    except Exception as e:
        return f"❌ Error en búsqueda masiva: {str(e)}"


@mcp.tool()
def bulk_create_reports(reports_data: list) -> str:
    """
    Crea múltiples reportes de una vez usando operaciones masivas

    Args:
        reports_data: Lista de diccionarios con datos de reportes. Cada diccionario debe contener:
            - report_prefix: Prefijo único del reporte
            - report_description_en: Descripción en inglés
            - report_description_es: Descripción en español
            - query: Query SQL (debe incluir @business_unit)
            - additional_params: Parámetros adicionales (opcional)
            - is_detail: Si es un detalle (0 o 1, opcional)
            - has_detail: Si tiene detalle (NULL, 0, 1, opcional)
            - action_column: Columna de acción (opcional)
            - detail_prefix: Prefijo del detalle (opcional)
            - detail_mode: Modo del detalle ('modal', 'page', opcional)
            - open_another_tab: Abrir en otra pestaña (0, 1, opcional)
            - type_resource: Tipo de recurso (por defecto 'table', opcional)
            - columns_to_render: Columnas a renderizar (NULL para todas, opcional)
            - default_for_all: Reporte por defecto para todas las empresas (0, 1, opcional)
            - database_key: Base de datos específica ('default' o 'INTEGRACION_CW_20_DEV', opcional)

    Returns:
        Resultado de la creación masiva de reportes
    """
    try:
        if not reports_data:
            return "⚠️ No se proporcionaron datos de reportes para crear"

        conn = get_db_connection("default")
        cursor = conn.cursor()

        successful_reports = []
        failed_reports = []
        total_created = 0

        for i, report_data in enumerate(reports_data, 1):
            try:
                # Validar campos requeridos
                required_fields = [
                    "report_prefix",
                    "report_description_en",
                    "report_description_es",
                    "query",
                ]
                missing_fields = [
                    field for field in required_fields if field not in report_data
                ]

                if missing_fields:
                    failed_reports.append(
                        {
                            "index": i,
                            "report_prefix": report_data.get("report_prefix", "N/A"),
                            "error": f"Campos faltantes: {', '.join(missing_fields)}",
                        }
                    )
                    continue

                # Validar que la query incluya @business_unit
                if "@business_unit" not in report_data["query"]:
                    failed_reports.append(
                        {
                            "index": i,
                            "report_prefix": report_data["report_prefix"],
                            "error": "La query debe incluir el parámetro @business_unit",
                        }
                    )
                    continue

                # Detectar automáticamente la base de datos si no se especifica
                query_database_key = report_data.get("database_key")
                if query_database_key is None:
                    if "INTEGRACION_CW_20_DEV" in report_data["query"].upper():
                        query_database_key = "INTEGRACION_CW_20_DEV"
                    else:
                        query_database_key = "default"

                # Construir params_config
                params_config = {"business_unit": "NVARCHAR(20)"}
                if report_data.get("additional_params"):
                    params_config.update(report_data["additional_params"])

                params_json = json.dumps(params_config)

                # Ejecutar el SP para crear el reporte
                sp_call = """
                EXEC sp_ecosystem_create_columns_config
                    ?, -- @report_prefix
                    ?, -- @report_description
                    ?, -- @report_description_es
                    'report', -- @type
                    1, -- @table_number
                    ?, -- @query
                    ?, -- @params_config
                    ?, -- @is_detail
                    ?, -- @hasDetail
                    ?, -- @actionColumn
                    ?, -- @detail_prefix
                    ?, -- @detail_mode
                    ?, -- @open_another_tab
                    ?, -- @type_resource
                    ?, -- @columns_to_render
                    ?  -- default_for_all
                """

                cursor.execute(
                    sp_call,
                    (
                        report_data["report_prefix"],
                        report_data["report_description_en"],
                        report_data["report_description_es"],
                        report_data["query"],
                        params_json,
                        report_data.get("is_detail", 0),
                        report_data.get("has_detail"),
                        report_data.get("action_column"),
                        report_data.get("detail_prefix"),
                        report_data.get("detail_mode"),
                        report_data.get("open_another_tab"),
                        report_data.get("type_resource", "table"),
                        report_data.get("columns_to_render"),
                        report_data.get("default_for_all", 0),
                    ),
                )

                successful_reports.append(
                    {
                        "index": i,
                        "report_prefix": report_data["report_prefix"],
                        "query_database": DB_CONFIGS[query_database_key]["database"],
                        "sp_database": DB_CONFIGS["default"]["database"],
                    }
                )
                total_created += 1

            except Exception as e:
                failed_reports.append(
                    {
                        "index": i,
                        "report_prefix": report_data.get("report_prefix", "N/A"),
                        "error": str(e),
                    }
                )

        conn.commit()
        conn.close()

        # Construir resultado
        result = "📊 Creación masiva de reportes completada!\n\n"
        result += f"✅ Reportes creados exitosamente: {total_created}\n"
        result += f"❌ Reportes fallidos: {len(failed_reports)}\n\n"

        if successful_reports:
            result += "📋 Reportes creados:\n"
            for report in successful_reports:
                result += f"  {report['index']}. {report['report_prefix']}\n"
                result += f"     Query apunta a: {report['query_database']}\n"
                result += f"     SP ejecutado en: {report['sp_database']}\n\n"

        if failed_reports:
            result += "❌ Reportes fallidos:\n"
            for report in failed_reports:
                result += f"  {report['index']}. {report['report_prefix']}: {report['error']}\n"

        return result

    except Exception as e:
        return f"❌ Error en creación masiva: {str(e)}"


@mcp.tool()
def bulk_assign_reports_to_roles(assignments_data: list) -> str:
    """
    Asigna múltiples reportes a múltiples roles usando operaciones INSERT masivas

    Args:
        assignments_data: Lista de diccionarios con datos de asignaciones. Cada diccionario debe contener:
            - report_prefix: Prefijo del reporte
            - business_unit: Unidad de negocio
            - role_description: Descripción del rol (ej: "administrador") - el sistema buscará el código
            - application_type: Tipo de aplicación ('sales_force' o 'merchandising')
            - order: Orden de visualización (opcional)
            - custom_tag: Etiqueta personalizada (opcional)
            - sales_office: Oficina de ventas (opcional)
            - center_logistical: Centro logístico (opcional)

    Returns:
        Resultado de la asignación masiva
    """
    try:
        if not assignments_data:
            return "⚠️ No se proporcionaron datos de asignaciones"

        conn = get_db_connection("default")
        cursor = conn.cursor()

        successful_assignments = []
        failed_assignments = []
        skipped_assignments = []
        total_inserted = 0

        # Validar tipos de aplicación permitidos
        valid_app_types = ["sales_force", "merchandising"]

        for i, assignment_data in enumerate(assignments_data, 1):
            try:
                # Validar campos requeridos
                required_fields = [
                    "report_prefix",
                    "business_unit",
                    "role_description",
                    "application_type",
                ]
                missing_fields = [
                    field for field in required_fields if field not in assignment_data
                ]

                if missing_fields:
                    failed_assignments.append(
                        {
                            "index": i,
                            "report_prefix": assignment_data.get(
                                "report_prefix", "N/A"
                            ),
                            "business_unit": assignment_data.get(
                                "business_unit", "N/A"
                            ),
                            "error": f"Campos faltantes: {', '.join(missing_fields)}",
                        }
                    )
                    continue

                # Validar tipo de aplicación
                app_type = assignment_data["application_type"]
                if app_type not in valid_app_types:
                    failed_assignments.append(
                        {
                            "index": i,
                            "report_prefix": assignment_data["report_prefix"],
                            "business_unit": assignment_data["business_unit"],
                            "error": f"application_type debe ser 'sales_force' o 'merchandising', recibido: '{app_type}'",
                        }
                    )
                    continue

                # Buscar el código del rol por descripción
                role_query = """
                SELECT [code], [description], [application_type]
                FROM default_roles 
                WHERE business_unit = ? AND [description] LIKE ? AND application_type != 'sys_admin'
                """

                cursor.execute(
                    role_query,
                    (
                        assignment_data["business_unit"],
                        f"%{assignment_data['role_description']}%",
                    ),
                )
                roles = cursor.fetchall()

                if not roles:
                    failed_assignments.append(
                        {
                            "index": i,
                            "report_prefix": assignment_data["report_prefix"],
                            "business_unit": assignment_data["business_unit"],
                            "error": f"No se encontró rol con descripción '{assignment_data['role_description']}' (excluyendo sys_admin)",
                        }
                    )
                    continue

                # Usar el primer rol encontrado
                role_code, role_desc, role_app_type = roles[0]

                if len(roles) > 1:
                    warning_msg = f"Se encontraron múltiples roles. Usando: {role_code} - {role_desc}"
                else:
                    warning_msg = ""

                # Verificar si ya existe la asignación
                check_query = """
                SELECT COUNT(*) FROM assigned_reports 
                WHERE business_unit = ? AND report_prefix = ? AND [role] = ?
                """

                cursor.execute(
                    check_query,
                    (
                        assignment_data["business_unit"],
                        assignment_data["report_prefix"],
                        role_code,
                    ),
                )
                exists = cursor.fetchone()[0]

                if exists > 0:
                    skipped_assignments.append(
                        {
                            "index": i,
                            "report_prefix": assignment_data["report_prefix"],
                            "business_unit": assignment_data["business_unit"],
                            "role_code": role_code,
                            "reason": "Asignación ya existe",
                        }
                    )
                    continue

                # Insertar la nueva asignación
                insert_query = """
                INSERT INTO assigned_reports (
                    [role], report_prefix, business_unit, application_type, [order], 
                    custom_tag, sales_office, center_logistical
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """

                cursor.execute(
                    insert_query,
                    (
                        role_code,
                        assignment_data["report_prefix"],
                        assignment_data["business_unit"],
                        assignment_data["application_type"],
                        assignment_data.get("order"),
                        assignment_data.get("custom_tag"),
                        assignment_data.get("sales_office"),
                        assignment_data.get("center_logistical"),
                    ),
                )

                successful_assignments.append(
                    {
                        "index": i,
                        "report_prefix": assignment_data["report_prefix"],
                        "business_unit": assignment_data["business_unit"],
                        "role_code": role_code,
                        "role_description": role_desc,
                        "application_type": assignment_data["application_type"],
                        "warning": warning_msg,
                    }
                )
                total_inserted += 1

            except Exception as e:
                failed_assignments.append(
                    {
                        "index": i,
                        "report_prefix": assignment_data.get("report_prefix", "N/A"),
                        "business_unit": assignment_data.get("business_unit", "N/A"),
                        "error": str(e),
                    }
                )

        conn.commit()
        conn.close()

        # Construir resultado
        result = "📊 Asignación masiva de reportes completada!\n\n"
        result += f"✅ Asignaciones creadas: {total_inserted}\n"
        result += f"⏭️ Asignaciones omitidas (ya existían): {len(skipped_assignments)}\n"
        result += f"❌ Asignaciones fallidas: {len(failed_assignments)}\n\n"

        if successful_assignments:
            result += "📋 Asignaciones creadas:\n"
            for assignment in successful_assignments:
                result += f"  {assignment['index']}. {assignment['report_prefix']} → {assignment['role_code']} - {assignment['role_description']}\n"
                result += f"     Unidad: {assignment['business_unit']} | Tipo: {assignment['application_type']}\n"
                if assignment["warning"]:
                    result += f"     ⚠️ {assignment['warning']}\n"
                result += "\n"

        if skipped_assignments:
            result += "⏭️ Asignaciones omitidas:\n"
            for assignment in skipped_assignments:
                result += f"  {assignment['index']}. {assignment['report_prefix']} → {assignment['role_code']} ({assignment['reason']})\n"

        if failed_assignments:
            result += "\n❌ Asignaciones fallidas:\n"
            for assignment in failed_assignments:
                result += f"  {assignment['index']}. {assignment['report_prefix']} ({assignment['business_unit']}): {assignment['error']}\n"

        return result

    except Exception as e:
        return f"❌ Error en asignación masiva: {str(e)}"


@mcp.tool()
def bulk_get_report_assignments(
    report_prefixes: list, business_unit: str = None
) -> str:
    """
    Obtiene las asignaciones de múltiples reportes usando operaciones IN

    Args:
        report_prefixes: Lista de prefijos de reportes a consultar
        business_unit: Unidad de negocio (opcional, si no se especifica muestra todas)

    Returns:
        Lista de asignaciones de todos los reportes solicitados
    """
    try:
        if not report_prefixes:
            return "⚠️ No se proporcionaron prefijos de reportes"

        conn = get_db_connection("default")
        cursor = conn.cursor()

        # Crear placeholders para la consulta IN
        placeholders = ",".join(["?" for _ in report_prefixes])

        if business_unit:
            query = f"""
            SELECT ar.[role], ar.report_prefix, ar.business_unit, ar.application_type, 
                   ar.[order], ar.custom_tag, ar.sales_office, ar.center_logistical,
                   dr.[description] as role_description
            FROM assigned_reports ar
            LEFT JOIN default_roles dr ON ar.[role] = dr.[code] AND ar.business_unit = dr.business_unit
            WHERE ar.report_prefix IN ({placeholders}) AND ar.business_unit = ?
            ORDER BY ar.report_prefix, ar.[order], dr.[description]
            """
            cursor.execute(query, report_prefixes + [business_unit])
        else:
            query = f"""
            SELECT ar.[role], ar.report_prefix, ar.business_unit, ar.application_type, 
                   ar.[order], ar.custom_tag, ar.sales_office, ar.center_logistical,
                   dr.[description] as role_description
            FROM assigned_reports ar
            LEFT JOIN default_roles dr ON ar.[role] = dr.[code] AND ar.business_unit = dr.business_unit
            WHERE ar.report_prefix IN ({placeholders})
            ORDER BY ar.business_unit, ar.report_prefix, ar.[order], dr.[description]
            """
            cursor.execute(query, report_prefixes)

        assignments = cursor.fetchall()

        if not assignments:
            reports_str = ", ".join(report_prefixes)
            scope = (
                f"para la unidad {business_unit}"
                if business_unit
                else "en ninguna unidad"
            )
            return f"❌ No se encontraron asignaciones para los reportes '{reports_str}' {scope}"

        # Agrupar resultados por reporte
        reports_data = {}
        for assignment in assignments:
            (
                role,
                prefix,
                bu,
                app_type,
                order,
                tag,
                sales_office,
                center_log,
                role_desc,
            ) = assignment

            if prefix not in reports_data:
                reports_data[prefix] = []

            reports_data[prefix].append(
                {
                    "role": role,
                    "business_unit": bu,
                    "application_type": app_type,
                    "order": order,
                    "custom_tag": tag,
                    "sales_office": sales_office,
                    "center_logistical": center_log,
                    "role_description": role_desc,
                }
            )

        # Construir resultado
        result = "📋 Asignaciones de reportes solicitados:\n"
        result += f"📊 Reportes consultados: {', '.join(report_prefixes)}\n"
        if business_unit:
            result += f"🏢 Unidad: {business_unit}\n"
        result += "\n"

        total_assignments = 0
        for report_prefix in report_prefixes:
            if report_prefix in reports_data:
                assignments_list = reports_data[report_prefix]
                total_assignments += len(assignments_list)

                result += f"📄 Reporte: {report_prefix}\n"
                result += f"  📊 Total asignaciones: {len(assignments_list)}\n"

                for assignment in assignments_list:
                    result += f"    • Rol: {assignment['role']} - {assignment['role_description'] or 'N/A'}\n"
                    result += f"      Unidad: {assignment['business_unit']}\n"
                    result += f"      Tipo: {assignment['application_type'] or 'N/A'}\n"
                    result += f"      Orden: {assignment['order'] or 'N/A'}\n"
                    result += f"      Etiqueta: {assignment['custom_tag'] or 'N/A'}\n"
                    result += f"      Oficina: {assignment['sales_office'] or 'N/A'}\n"
                    result += (
                        f"      Centro: {assignment['center_logistical'] or 'N/A'}\n\n"
                    )
            else:
                result += f"❌ Reporte '{report_prefix}': Sin asignaciones\n\n"

        result += f"📈 Resumen: {total_assignments} asignaciones encontradas en total"

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al obtener asignaciones masivas: {str(e)}"


@mcp.tool()
def bulk_update_report_assignments(
    report_prefix: str,
    business_unit: str,
    role_codes: list,
    application_type: str = None,
    order: int = None,
    custom_tag: str = None,
    sales_office: str = None,
    center_logistical: str = None,
) -> str:
    """
    Actualiza múltiples asignaciones de reporte usando operaciones IN

    Args:
        report_prefix: Prefijo del reporte
        business_unit: Unidad de negocio
        role_codes: Lista de códigos de roles a actualizar
        application_type: Nuevo tipo de aplicación ('sales_force' o 'merchandising', opcional)
        order: Nuevo orden de visualización (opcional)
        custom_tag: Nueva etiqueta personalizada (opcional)
        sales_office: Nueva oficina de ventas (opcional)
        center_logistical: Nuevo centro logístico (opcional)

    Returns:
        Resultado de la actualización masiva
    """
    try:
        if not role_codes:
            return "⚠️ No se proporcionaron códigos de roles para actualizar"

        conn = get_db_connection("default")
        cursor = conn.cursor()

        # Paso 1: Verificar cuántas asignaciones existen
        placeholders = ",".join(["?" for _ in role_codes])
        check_query = f"""
        SELECT COUNT(*) FROM assigned_reports 
        WHERE business_unit = ? AND report_prefix = ? AND [role] IN ({placeholders})
        """

        check_values = [business_unit, report_prefix] + role_codes
        cursor.execute(check_query, check_values)
        exists_count = cursor.fetchone()[0]

        if exists_count == 0:
            return f"❌ No existen asignaciones del reporte '{report_prefix}' para los roles {', '.join(role_codes)} en la unidad {business_unit}"

        # Paso 2: Obtener información de los roles que se van a actualizar
        roles_query = f"""
        SELECT ar.[role], dr.[description] as role_description
        FROM assigned_reports ar
        LEFT JOIN default_roles dr ON ar.[role] = dr.[code] AND ar.business_unit = dr.business_unit
        WHERE ar.business_unit = ? AND ar.report_prefix = ? AND ar.[role] IN ({placeholders})
        ORDER BY ar.[role]
        """

        cursor.execute(roles_query, check_values)
        roles_info = cursor.fetchall()

        # Paso 3: Construir la query de actualización
        update_fields = []
        update_values = []

        if application_type is not None:
            update_fields.append("application_type = ?")
            update_values.append(application_type)

        if order is not None:
            update_fields.append("[order] = ?")
            update_values.append(order)

        if custom_tag is not None:
            update_fields.append("custom_tag = ?")
            update_values.append(custom_tag)

        if sales_office is not None:
            update_fields.append("sales_office = ?")
            update_values.append(sales_office)

        if center_logistical is not None:
            update_fields.append("center_logistical = ?")
            update_values.append(center_logistical)

        if not update_fields:
            return "⚠️ No se proporcionaron campos para actualizar"

        # Agregar los valores de WHERE
        update_values.extend([business_unit, report_prefix] + role_codes)

        update_query = f"""
        UPDATE assigned_reports 
        SET {", ".join(update_fields)}
        WHERE business_unit = ? AND report_prefix = ? AND [role] IN ({placeholders})
        """

        cursor.execute(update_query, update_values)
        affected_rows = cursor.rowcount
        conn.commit()
        conn.close()

        # Construir mensaje de resultado
        roles_text = "\n  • ".join(
            [f"{role[0]} - {role[1] or 'N/A'}" for role in roles_info]
        )

        changes_text = []
        if application_type is not None:
            changes_text.append(f"Tipo de aplicación: → {application_type}")
        if order is not None:
            changes_text.append(f"Orden: → {order}")
        if custom_tag is not None:
            changes_text.append(f"Etiqueta: → {custom_tag}")
        if sales_office is not None:
            changes_text.append(f"Oficina de ventas: → {sales_office}")
        if center_logistical is not None:
            changes_text.append(f"Centro logístico: → {center_logistical}")

        changes_display = (
            "\n  • ".join(changes_text) if changes_text else "Sin cambios detectados"
        )

        return f"""✅ Actualización masiva completada exitosamente!

📋 Detalles de la actualización:
  • Reporte: {report_prefix}
  • Unidad: {business_unit}
  • Registros actualizados: {affected_rows}
  
👥 Roles actualizados:
  • {roles_text}
  
🔄 Cambios aplicados:
  • {changes_display}
"""

    except Exception as e:
        return f"❌ Error en actualización masiva: {str(e)}"


@mcp.tool()
def update_report_assignment(
    report_prefix: str,
    business_unit: str,
    role_code: str,
    application_type: str = None,
    order: int = None,
    custom_tag: str = None,
    sales_office: str = None,
    center_logistical: str = None,
) -> str:
    """
    Actualiza una asignación existente de reporte en la tabla assigned_reports

    Args:
        report_prefix: Prefijo del reporte
        business_unit: Unidad de negocio
        role_code: Código del rol a actualizar
        application_type: Nuevo tipo de aplicación ('sales_force' o 'merchandising', opcional)
        order: Nuevo orden de visualización (opcional)
        custom_tag: Nueva etiqueta personalizada (opcional)
        sales_office: Nueva oficina de ventas (opcional)
        center_logistical: Nuevo centro logístico (opcional)

    Returns:
        Resultado de la actualización
    """
    try:
        conn = get_db_connection("default")
        cursor = conn.cursor()

        # Paso 1: Verificar si existe la asignación
        check_query = """
        SELECT COUNT(*) FROM assigned_reports 
        WHERE business_unit = ? AND report_prefix = ? AND [role] = ?
        """

        cursor.execute(check_query, (business_unit, report_prefix, role_code))
        exists = cursor.fetchone()[0]

        if exists == 0:
            return f"❌ No existe una asignación del reporte '{report_prefix}' para el rol '{role_code}' en la unidad {business_unit}"

        # Paso 2: Obtener la asignación actual para mostrar qué se va a cambiar
        current_query = """
        SELECT ar.application_type, ar.[order], ar.custom_tag, ar.sales_office, ar.center_logistical,
               dr.[description] as role_description
        FROM assigned_reports ar
        LEFT JOIN default_roles dr ON ar.[role] = dr.[code] AND ar.business_unit = dr.business_unit
        WHERE ar.business_unit = ? AND ar.report_prefix = ? AND ar.[role] = ?
        """

        cursor.execute(current_query, (business_unit, report_prefix, role_code))
        current = cursor.fetchone()

        if not current:
            return "❌ Error al obtener la asignación actual"

        (
            current_app_type,
            current_order,
            current_tag,
            current_sales_office,
            current_center_log,
            role_desc,
        ) = current

        # Paso 3: Construir la query de actualización solo con los campos proporcionados
        update_fields = []
        update_values = []

        if application_type is not None:
            update_fields.append("application_type = ?")
            update_values.append(application_type)

        if order is not None:
            update_fields.append("[order] = ?")
            update_values.append(order)

        if custom_tag is not None:
            update_fields.append("custom_tag = ?")
            update_values.append(custom_tag)

        if sales_office is not None:
            update_fields.append("sales_office = ?")
            update_values.append(sales_office)

        if center_logistical is not None:
            update_fields.append("center_logistical = ?")
            update_values.append(center_logistical)

        if not update_fields:
            return "⚠️ No se proporcionaron campos para actualizar"

        # Agregar los valores de WHERE
        update_values.extend([business_unit, report_prefix, role_code])

        update_query = f"""
        UPDATE assigned_reports 
        SET {", ".join(update_fields)}
        WHERE business_unit = ? AND report_prefix = ? AND [role] = ?
        """

        cursor.execute(update_query, update_values)
        conn.commit()
        conn.close()

        # Construir mensaje de cambios
        changes = []
        if application_type is not None and application_type != current_app_type:
            changes.append(
                f"Tipo de aplicación: {current_app_type} → {application_type}"
            )
        if order is not None and order != current_order:
            changes.append(f"Orden: {current_order} → {order}")
        if custom_tag is not None and custom_tag != current_tag:
            changes.append(f"Etiqueta: {current_tag} → {custom_tag}")
        if sales_office is not None and sales_office != current_sales_office:
            changes.append(
                f"Oficina de ventas: {current_sales_office} → {sales_office}"
            )
        if center_logistical is not None and center_logistical != current_center_log:
            changes.append(
                f"Centro logístico: {current_center_log} → {center_logistical}"
            )

        changes_text = "\n  • ".join(changes) if changes else "Sin cambios detectados"

        return f"""✅ Asignación actualizada exitosamente!

📋 Detalles de la actualización:
  • Reporte: {report_prefix}
  • Unidad: {business_unit}
  • Rol: {role_code} - {role_desc or "N/A"}
  
🔄 Cambios realizados:
  • {changes_text}
"""

    except Exception as e:
        return f"❌ Error al actualizar asignación: {str(e)}"


@mcp.tool()
def list_available_roles(business_unit: str) -> str:
    """
    Lista todos los roles disponibles para una unidad de negocio (excluyendo sys_admin)

    Args:
        business_unit: Unidad de negocio

    Returns:
        Lista de roles disponibles con sus códigos y descripciones
    """
    try:
        conn = get_db_connection("default")
        cursor = conn.cursor()

        query = """
        SELECT [code], [description], [application_type], [order_]
        FROM default_roles 
        WHERE business_unit = ? AND application_type != 'sys_admin'
        ORDER BY [order_], [description]
        """

        cursor.execute(query, business_unit)
        roles = cursor.fetchall()

        if not roles:
            return f"❌ No se encontraron roles para la unidad {business_unit} (excluyendo sys_admin)"

        result = f"👥 Roles disponibles para la unidad {business_unit}:\n\n"
        for role in roles:
            code, description, app_type, order = role
            result += (
                f"  • {code} - {description} ({app_type}) [Orden: {order or 'N/A'}]\n"
            )

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al listar roles: {str(e)}"


@mcp.tool()
def get_report_assignments(report_prefix: str, business_unit: str = None) -> str:
    """
    Obtiene las asignaciones existentes de un reporte

    Args:
        report_prefix: Prefijo del reporte
        business_unit: Unidad de negocio (opcional, si no se especifica muestra todas)

    Returns:
        Lista de asignaciones del reporte
    """
    try:
        conn = get_db_connection("default")
        cursor = conn.cursor()

        if business_unit:
            query = """
            SELECT ar.[role], ar.report_prefix, ar.business_unit, ar.application_type, 
                   ar.[order], ar.custom_tag, ar.sales_office, ar.center_logistical,
                   dr.[description] as role_description
            FROM assigned_reports ar
            LEFT JOIN default_roles dr ON ar.[role] = dr.[code] AND ar.business_unit = dr.business_unit
            WHERE ar.report_prefix = ? AND ar.business_unit = ?
            ORDER BY ar.[order], dr.[description]
            """
            cursor.execute(query, (report_prefix, business_unit))
        else:
            query = """
            SELECT ar.[role], ar.report_prefix, ar.business_unit, ar.application_type, 
                   ar.[order], ar.custom_tag, ar.sales_office, ar.center_logistical,
                   dr.[description] as role_description
            FROM assigned_reports ar
            LEFT JOIN default_roles dr ON ar.[role] = dr.[code] AND ar.business_unit = dr.business_unit
            WHERE ar.report_prefix = ?
            ORDER BY ar.business_unit, ar.[order], dr.[description]
            """
            cursor.execute(query, report_prefix)

        assignments = cursor.fetchall()

        if not assignments:
            scope = (
                f"para la unidad {business_unit}"
                if business_unit
                else "en ninguna unidad"
            )
            return f"❌ No se encontraron asignaciones para el reporte '{report_prefix}' {scope}"

        result = f"📋 Asignaciones del reporte '{report_prefix}':\n\n"
        for assignment in assignments:
            (
                role,
                prefix,
                bu,
                app_type,
                order,
                tag,
                sales_office,
                center_log,
                role_desc,
            ) = assignment
            result += f"🏢 Unidad: {bu}\n"
            result += f"  • Rol: {role} - {role_desc or 'N/A'}\n"
            result += f"  • Tipo de aplicación: {app_type or 'N/A'}\n"
            result += f"  • Orden: {order or 'N/A'}\n"
            result += f"  • Etiqueta: {tag or 'N/A'}\n"
            result += f"  • Oficina de ventas: {sales_office or 'N/A'}\n"
            result += f"  • Centro logístico: {center_log or 'N/A'}\n\n"

        conn.close()
        return result

    except Exception as e:
        return f"❌ Error al obtener asignaciones: {str(e)}"


@mcp.tool()
def search_table_in_all_databases(table_name: str) -> str:
    """
    Busca una tabla en todas las bases de datos configuradas

    Args:
        table_name: Nombre de la tabla a buscar

    Returns:
        Información sobre la tabla encontrada en cada base de datos
    """
    try:
        result = f"🔍 Buscando tabla '{table_name}' en todas las bases de datos:\n\n"

        for db_key, db_config in DB_CONFIGS.items():
            try:
                conn = get_db_connection(db_key)
                cursor = conn.cursor()

                query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """

                cursor.execute(query, table_name)
                columns = cursor.fetchall()

                if columns:
                    result += f"✅ Encontrada en {db_config['database']}:\n"
                    for col in columns:
                        col_name, data_type, max_length, nullable = col
                        length_info = f"({max_length})" if max_length else ""
                        null_info = "NULL" if nullable == "YES" else "NOT NULL"
                        result += (
                            f"  • {col_name}: {data_type}{length_info} - {null_info}\n"
                        )
                    result += "\n"
                else:
                    result += f"❌ No encontrada en {db_config['database']}\n\n"

                conn.close()

            except Exception as e:
                result += f"⚠️ Error al consultar {db_config['database']}: {str(e)}\n\n"

        return result

    except Exception as e:
        return f"❌ Error general: {str(e)}"


if __name__ == "__main__":
    import sys
    from fastmcp.cli.run import run_command

    if len(sys.argv) > 1 and sys.argv[1] == "--http":
        # Modo HTTP para exposición web usando FastMCP CLI
        run_command(server_spec="echo.py", transport="http", host="0.0.0.0", port=9095)
    else:
        # Modo stdio para Claude Desktop
        mcp.run(transport="stdio")
