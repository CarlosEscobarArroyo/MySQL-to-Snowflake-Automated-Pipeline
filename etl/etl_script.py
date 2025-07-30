import pymysql
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import date
import numpy as np
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/airflow/.env")


def extraer_datos_mysql():
    """
    Extrae datos de las tablas necesarias desde una base de datos MySQL.

    Establece conexión con una base de datos MySQL utilizando pymysql, 
    y extrae las siguientes tablas en forma de DataFrames de pandas:
    - persona
    - contenido
    - campana
    - documento
    - documentodetalle

    Retorna:
    -------
    dict
        Un diccionario donde las claves son los nombres lógicos de las tablas
        y los valores son los DataFrames extraídos.

    Manejo de errores:
    -----------------
    Si la conexión falla o ocurre una excepción durante la lectura, se imprime
    un mensaje de error y no se retorna ningún valor.
    """
    try:
        conn = pymysql.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            port=int(os.getenv("MYSQL_PORT"))
        )

        print("✅ Conexión exitosa a la base de datos (pymysql)")
        persona = pd.read_sql("SELECT * FROM persona", conn)
        contenido = pd.read_sql("SELECT * FROM contenido", conn)
        campana = pd.read_sql("SELECT * FROM campana", conn)
        documento = pd.read_sql("SELECT * FROM documento", conn)
        documentodetalle = pd.read_sql("SELECT * FROM documentodetalle", conn)
        print("✅ Datos cargados en DataFrames")
        
        conn.close()
        
        return {
            "persona": persona,
            "contenido": contenido,
            "campana": campana,
            "documento": documento,
            "documentodetalle": documentodetalle
        }
        
    except Exception as e:
        print("❌ Error al conectar:", e)
        
def limpiar_tablas(persona, contenido, campana, documento, documentodetalle):
    """
    Realiza la limpieza, transformación y enriquecimiento de los datos extraídos.

    Aplica las siguientes operaciones:
    - Eliminación de columnas irrelevantes.
    - Renombrado de columnas para estandarización.
    - Clasificación de productos en categorías y subcategorías según nombre.
    - Conversión de fechas y cálculo de edad.
    - Construcción de dimensiones: VENDEDOR, COORDINADORA, PRODUCTO, FECHA, etc.
    - Limpieza y preparación de tablas STG para transacciones.

    Parámetros:
    ----------
    persona : pd.DataFrame
        Tabla con datos personales de vendedores y coordinadoras.
    contenido : pd.DataFrame
        Tabla con información de productos.
    campana : pd.DataFrame
        Tabla de campañas comerciales.
    documento : pd.DataFrame
        Tabla de pedidos.
    documentodetalle : pd.DataFrame
        Tabla de detalles de pedidos.

    Retorna:
    -------
    dict
        Diccionario con los DataFrames transformados y listos para carga:
        - DIM_CAMPANA
        - DIM_PRODUCTO
        - DIM_UBICACION
        - DIM_FECHA
        - DIM_VENDEDOR
        - DIM_COORDINADORA
        - STG_DOCUMENTO
        - STG_DOCUMENTODETALLE

    Requiere:
    --------
    - Archivos locales 'tabla_cruzada.xlsx' y 'dimdate.xlsx' para ubigeo y fechas.
    """

    # Limpieza tabla contenido
    contenido.drop(columns=['ccodpage', 'ccodpersona', 'ccodmodulo', 'ccodestcontenido', 'ctagcontenido', 'cimgcontenido', 'cbancontenido', 'cestcontenido', 'estienda', 'ctipoperacion',
                        'ctipcontenido', 'ctipenlace', 'curlenlace', 'ccodinterno', 'ccodmoneda', 'ccodunidad', 'nprecompra', 'npremayor', 'nstoproducto', 'catrproducto',
                        'cestproducto', 'cestcomentario', 'copccontenido', 'copcpaquete', 'copcinmueble', 'ndiapaquete', 'nareainmueble', 'nconsinmueble', 'nviscontenido',
                        'dfecinicio', 'dfecfin', 'dfeccontenido', 'ccodusuario', 'dfecmodifica', 'csubcontenido'], inplace=True)
    contenido.rename(columns={'ccodcontenido': 'id_producto','cnomcontenido': 'nombre_producto'}, inplace=True)
    import numpy as np

    # Define las categorías y sus palabras clave asociadas
    categorias = {
        "Ropa": [
            "blusa", "vestido", "saco", "pantalon", "pantalón", "camiseta", "básico",
            "batola", "camisilla", "chaleco", "chaqueta", "conjunto", "enterizo", "polo",
            "camisa", "top", "leggin", "short", "capri", "chompa", "kimono", "cardigan",
            "abrigo", "pijama", "capa", "casaca", "botines", "sandalias", "zapatillas",
            "zapatos", "sniker", "falda", "faldon", "jean", "pantalaon", "bikini",
            "baby doll", "brasier", "panty", "mandil", "polera", "chompera", "sandalia",
            "chauqueta", "ballerinas", "zapato", "botin", "pantufla", "camison", "blazer", "bata"
        ],
        "Maquillaje": [
            "labial", "lipgloss", "lipstick", "lipstisk", "rubor", "delineador",
            "rimel", "cejas", "sombras", "polvo", "corrector", "brochas"
        ],
        "Perfumes": [
            "perfume", "esz", "kp"
        ],
        "Accesorios": [
            "arete", "collar", "anillo", "morral", "cartera", "bolsa", "lentes", "reloj",
            "correa", "mascarilla", "cubretodo", "pulsera", "tobillera", "monederos",
            "mochil", "billetera", "portacelular"
        ],
        "Electrodomésticos": [
            "horno", "parrilla", "olla", "fuente", "jarra", "taza", "dulcera", "plato",
            "vaso", "termo", "copas", "vajilla", "moldes", "portacucharas", "cocina",
            "secadora", "electrodomesticos", "plancha", "miniplancha", "exprimidor",
            "microondas", "harina", "batidor", "jarrita", "taper", "salero", "lampara",
            "papelero", "herramienta", "frigobar", "tv", "espejo"
        ],
        "Regalos / Incentivos": [
            "regalo", "cartuchera", "gana", "promocion", "pc", "premio"
        ],
        "Aceites Esenciales": [
            "ae"
        ]
    }

    # Flatten para condiciones y valores
    condiciones = []
    valores = []
    # Iterar sobre las categorías y sus palabras clave
    for categoria, palabras in categorias.items():
        for palabra in palabras:
            condiciones.append(contenido['nombre_producto'].str.lower().str.contains(palabra))
            valores.append(categoria)

    # Clasificar con np.select
    contenido['categoria'] = np.select(condiciones, valores, default='Sin clasificar')

    # Asegúrate de tener tu DataFrame llamado 'contenido'
    contenido['nombre_producto'] = contenido['nombre_producto'].str.lower().fillna("")

    # Diccionario de subcategorías
    subcategorias = {
        "Regalos": ["pc", "premio"],
        "Blusas": ["blusa"],
        "Vestidos": ["vestido"],
        "Sacos": ["saco"],
        "Pantalones": ["pantalon", "pantalón", "jeans", "pantalaon", "jean"],
        "Básicos": ["básico", "camiseta"],
        "Batolas": ["batola"],
        "Camisillas": ["camisilla"],
        "Chalecos": ["chaleco"],
        "Chaquetas": ["chaqueta", "chauqueta"],
        "Conjuntos": ["conjunto"],
        "Enterizos": ["enterizo"],
        "Polos": ["polo"],
        "Camisas": ["camisa"],
        "Tops": ["top"],
        "Leggins": ["leggin"],
        "Shorts": ["short"],
        "Capris": ["capri"],
        "Chompas": ["chompa"],
        "Capas": ["kimono", "capa"],
        "Cardigans": ["cardigan"],
        "Abrigos": ["abrigo"],
        "Pijamas": ["pijama", "camison"],
        "Labios": ["labial", "lipgloss", "lipstick"],
        "Rubor": ["rubor"],
        "Ojos": ["delineador", "rimel", "sombras"],
        "Cejas": ["cejas"],
        "Polvo": ["polvo"],
        "Corrector": ["corrector"],
        "Brochas": ["brochas"],
        "Aretes": ["arete"],
        "Collares": ["collar"],
        "Anillos": ["anillo"],
        "Carteras": ["morral", "mochil", "cartera", "bolsa"],
        "Lentes": ["lentes"],
        "Relojes": ["reloj"],
        "Correas": ["correa"],
        "Mascarillas": ["mascarilla"],
        "Cubretodo": ["cubretodo"],
        "Pulseras": ["pulsera"],
        "Tobilleras": ["tobillera"],
        "Otros": ["monederos", "billetera", "portacelular", "mandil"],
        "Hogar": ["toalla", "sabana", "edredón", "portaretrato", "lampara", "papelero", "herramienta", "tv", "espejo"],
        "Cocina": ["jarra", "taza", "dulcera", "plato", "vaso", "termo", "olla", "horno", "parrilla", "fuente", "copas", "vajilla", "moldes", "portacucharas", "cocina", "frigobar", "exprimidor", "microondas", "harina", "batidor", "jarrita", "taper", "salero"],
        "Sin subcategoría": ["secadora", "electrodomesticos", "plancha", "miniplancha"],
        "Calzado": ["botines", "sandalias", "zapatillas", "zapatos", "sniker", "sandalia", "botin", "ballerinas", "pantufla", "zapato"],
        "Faldas": ["falda", "faldon"],
        "Bikini": ["bikini"],
        "Lencería": ["baby doll", "brasier", "panty"],
        "Poleras": ["polera", "chompera"],
        "Masculino": ["esz"],
        "Femenino": ["kp"],
        "Blazer": ["blazer"],
        "Bata": ["bata"],
        "Aceites Esenciales": ["ae"],
        "Casacas": ["casaca"]
    }

    # Crear condiciones y valores
    condiciones = []
    valores = []

    for categoria, keywords in subcategorias.items():
        for palabra in keywords:
            condiciones.append(contenido['nombre_producto'].str.contains(palabra, na=False))
            valores.append(categoria)

    # Asignar nueva columna
    contenido['subcategoria'] = np.select(condiciones, valores, default='Sin subcategoría')

    contenido.drop(columns=['camicontenido', 'crescontenido', 'ccodcategoria', 'npreproducto'], inplace=True)
    
    # Limpieza tabla campaña
    campana.drop(columns=['ctipcampana', 'cordcampana', 'ccodusuario', 'dfecmodifica'], inplace=True)
    campana.rename(columns={'ccodcampana': 'id_campana'}, inplace=True)
    campana.rename(columns={'cnomcampana': 'nombre_campana'}, inplace=True)
    campana.rename(columns={'cestcampana': 'estado'}, inplace=True)
    campana.rename(columns={'dfecinicio': 'fecha_inicio'}, inplace=True)
    campana.rename(columns={'dfecfinal': 'fecha_fin'}, inplace=True)

    # Limpieza de erorres especificos en la tabla campana
    campana.loc[campana["nombre_campana"] == "Glamour 2017-08", "fecha_fin"] = "2017-10-09"
    campana.loc[campana["nombre_campana"] == "2019 Liquidacion 01 ", "fecha_inicio"] = "2019-01-05"
    campana.loc[campana["nombre_campana"] == "ROMA 2019-01", "fecha_fin"] = "2019-03-25"
    campana.loc[campana["nombre_campana"] == "CAMBIOS Y DEVOLUCIONES", "fecha_fin"] = "2020-01-30"
    campana.loc[campana["nombre_campana"] == "ESENZA C1 PERFUMES DEL MUNDO", "fecha_fin"] = "2023-09-11"
    campana.loc[campana["nombre_campana"] == "2019 C 09 LIQUIDACIÓN TIENDA", "fecha_fin"] = "2019-12-19"
    
    campana['fecha_inicio'] = pd.to_datetime(campana['fecha_inicio'], errors='coerce')
    campana['fecha_fin'] = pd.to_datetime(campana['fecha_fin'], errors='coerce')
    campana['anio'] = campana['fecha_inicio'].dt.year
    
    # Limpieza tabla persona
    persona.drop(columns=['cnikpersona', 'cpaspersona', 'cestsuscripcion', 'ccodgrupo', 'ctipdocumento', 'crucpersona', 'crazempresa', 'cdirempresa', 'cubiempresa', 'cimgpersona', 'ccodpostal',
                      'cnomzona', 'creferencia', 'ntelefono', 'ntelefono2', 'nmovil2', 'nsalfavor', 'ccodidioma', 'cclifactura', 'nvispersona', 'ccodautenticacion',
                      'dlogpersona', 'dfecmodifica', 'ccodusuario'], inplace=True)
    persona['dnacpersona'] = pd.to_datetime(persona['dnacpersona'], errors='coerce')
    persona['dfecpersona'] = pd.to_datetime(persona['dfecpersona'], errors='coerce')

    today = pd.to_datetime(date.today())
    persona['edad'] = (today - persona['dnacpersona']).dt.days // 365
    
    # Tabla Coordinadoras
    coordinadoras = persona[persona["cnivpersona"] == '5']
    coordinadoras.drop(columns=['ccodubigeo','cemapersona', 'cnivpersona', 'csexpersona', 'cdnipersona', 'dnacpersona', 'cdireccion', 'nmovil', 'ccodrelacion', 'ccodrelacion2', 'ccodasesor', 'dfecpersona'], inplace=True)
    coordinadoras.rename(columns={'ccodpersona': 'id_coordinadora'}, inplace=True)
    coordinadoras.rename(columns={'cnompersona': 'nombre_coordinadora'}, inplace=True)
    coordinadoras.rename(columns={'cestpersona': 'estado'}, inplace=True)
    
    # Tabla Lideres
    lideres = persona[persona["cnivpersona"] == '3']
    lideres.drop(columns=['cemapersona', 'cnivpersona', 'cestpersona', 'cdnipersona', 'cdireccion', 'ccodrelacion2', 'dnacpersona'], inplace=True)
    lideres.rename(columns={'ccodpersona': 'id_vendedor'}, inplace=True)
    lideres.rename(columns={'cnompersona': 'nombre_vendedor'}, inplace=True)
    lideres.rename(columns={'ccodasesor': 'id_coordinadora'}, inplace=True)
    lideres.rename(columns={'dfecpersona': 'fecha_ingreso'}, inplace=True)
    lideres['tipo_vendedor'] = 'Líder'
    lideres['ccodrelacion'] = 0
    
    # Tabla Asesoras
    asesoras = persona[persona["cnivpersona"] == '2']
    asesoras.drop(columns=['cemapersona', 'cnivpersona', 'cestpersona', 'cdnipersona', 'cdireccion', 'ccodrelacion2', 'dnacpersona'], inplace=True)
    asesoras.rename(columns={'ccodpersona': 'id_vendedor'}, inplace=True)
    asesoras.rename(columns={'cnompersona': 'nombre_vendedor'}, inplace=True)
    asesoras.rename(columns={'ccodasesor': 'id_coordinadora'}, inplace=True)
    asesoras.rename(columns={'dfecpersona': 'fecha_ingreso'}, inplace=True)
    asesoras['tipo_vendedor'] = 'Asesora'
    
    # Creacion tabla vendedores
    vendedores = pd.concat([asesoras, lideres], ignore_index=True)
    vendedores['id_coordinadora'] = vendedores['id_coordinadora'].astype(str)
    coordinadoras['id_coordinadora'] = coordinadoras['id_coordinadora'].astype(str)
    ids_validos = set(coordinadoras['id_coordinadora'])
    vendedores['id_coordinadora'] = vendedores['id_coordinadora'].apply(lambda x: x if x in ids_validos else '0')
    vendedores["fecha_ingreso"] = pd.to_datetime(vendedores["fecha_ingreso"])
    vendedores["fecha_ingreso"] = vendedores["fecha_ingreso"].dt.date
    
    # Limpieza tabla documento
    documento = documento[documento['cestpedido'] != '9']
    documento['Tipo'] = documento['nmoncomision'].apply(lambda x: 'Directo' if x == 0 else 'Catalogo')
    documento.drop(columns=['ccodpage', 'nmoncomision', 'nmonenvio',
       'nporpago', 'nmondevuelto', 'ctipenvio', 'cdespedido',
       'dfecpago', 'ccodpago', 'copepago', 'nsalpago', 'dfecconfirma',
       'cclifactura', 'dfecfactura', 'ctipfactura', 'cserfactura',
       'cnrofactura', 'dfecentrega', 'ctipentrega', 'cdocentrega',
       'dfecdevolucion', 'cestcredito', 'cserncredito', 'cnumncredito',
       'dfecncredito', 'ccodnota', 'dfecanulacion', 'ccodusuario',
       'dfecmodifica', 'ccodlider', 'cestpedido'], inplace=True)
    documento.rename(columns={'ccoddocumento': 'id_documento'}, inplace=True)
    documento['dfecpedido'] = pd.to_datetime(documento['dfecpedido'], errors='coerce')
    documento["dfecpedido"] = pd.to_datetime(documento["dfecpedido"], unit='ns', errors="coerce")
    documento["dfecpedido"] = documento["dfecpedido"].dt.date

    # Limpieza tabla documentodetalle
    documentodetalle.drop(columns=['ccodcolor', 'ccodtalla',
       'cnomunidad', 'ccategoria', 'nprecio', 'nprepromo',
        'wstotal', 'ccodusuario', 'dfecmodifica', 'estado'], inplace=True)
    documentodetalle.rename(columns={'ccoddetalle': 'id_detalle'}, inplace=True)
    documentodetalle.rename(columns={'ccoddetalle': 'id_contenido'}, inplace=True)
    documentodetalle = documentodetalle[documentodetalle['ccodpedido'].isin(documento['ccodpedido'])]
    
    # Tabla ubicacion
    ubicacion = pd.read_excel("/opt/airflow/etl/tabla_cruzada.xlsx")
    ubicacion.drop(columns=['cnivubigeo', 'cnomubigeo_distrito', 'cod_dep', 'cnomubigeo_departamento'], inplace=True)
    ubicacion = ubicacion.drop_duplicates(subset='ccodubigeo', keep='first')
    
    # Tabla fecha
    fecha = pd.read_excel("/opt/airflow/etl/dimdate.xlsx")
    fecha = fecha[['Id', 'Date', 'CalendarYear', 'CalendarMonth', 'CalendarDayInMonth', 'CalendarQuarter']]
    fecha["Date"] = pd.to_datetime(fecha["Date"])
    fecha["Date"] = fecha["Date"].dt.date
    
    return {
        "DIM_CAMPANA": campana,
        "DIM_PRODUCTO": contenido,
        "DIM_UBICACION": ubicacion,
        "DIM_FECHA": fecha,
        "DIM_VENDEDOR": vendedores,
        "DIM_COORDINADORA": coordinadoras,
        "STG_DOCUMENTO": documento,
        "STG_DOCUMENTODETALLE": documentodetalle
    }
        
def cargar_tablas_en_snowflake(tablas: dict):
    """
    Carga los DataFrames procesados a Snowflake en las tablas correspondientes.

    Establece conexión con Snowflake utilizando variables de entorno cargadas desde .env.
    Luego, itera sobre el diccionario de DataFrames y carga cada uno en su tabla destino
    usando el método `write_pandas`, sobrescribiendo el contenido anterior.

    Parámetros:
    ----------
    tablas : dict
        Diccionario donde cada clave es el nombre de la tabla destino en Snowflake 
        (ej. "DIM_CAMPANA") y el valor es un DataFrame de pandas.

    Requiere variables en el archivo .env:
    -------------------------------------
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA

    Retorna:
    -------
    None

    Manejo de errores:
    -----------------
    Si la conexión o la carga fallan, se imprime un mensaje de error con la excepción.
    """

    # Cargar variables desde .env
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path)
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        
        print("✅ Conectado a Snowflake")

        for nombre_tabla, df in tablas.items():
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name=nombre_tabla,
                overwrite=True
            )
            if success:
                print(f"✅ Cargado {nrows} filas en {nombre_tabla}")
            else:
                print(f"❌ Falló la carga de {nombre_tabla}")

        conn.close()

    except Exception as e:
        print(f"❌ Error al cargar en Snowflake: {e}")

def ejecutar_etl():
    """
    Orquesta el flujo completo del proceso ETL desde MySQL hasta Snowflake.

    Pasos:
    ------
    1. Extrae los datos crudos desde una base de datos MySQL.
    2. Realiza la limpieza, transformación y enriquecimiento de los datos.
    3. Carga los DataFrames resultantes a las tablas destino en Snowflake.

    Este flujo depende de:
    - Conexión válida a MySQL y Snowflake.
    - Archivos auxiliares 'tabla_cruzada.xlsx' y 'dimdate.xlsx'.
    - Variables de entorno correctamente configuradas (.env).

    Manejo de errores:
    -----------------
    Detiene el proceso si la extracción de datos falla.
    """    
    datos_raw = extraer_datos_mysql()
    tablas_limpias = limpiar_tablas(**datos_raw)
    cargar_tablas_en_snowflake(tablas_limpias)

if __name__ == "__main__":
    ejecutar_etl()
    print("✅ Proceso ETL completado exitosamente.")