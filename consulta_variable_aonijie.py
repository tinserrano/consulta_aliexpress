from iop.base import IopClient, IopRequest
import pandas as pd
import json
import time
import os
from datetime import datetime
import variablesid2
from supabase import create_client

def buscar_productos_completos(keywords, archivo_salida=None):
    # Configuración inicial
    app_key = variablesid2.app_key
    app_secret = variablesid2.app_secret
    server_url = variablesid2.server_url

    # Configuración de Supabase
    supabase_url = variablesid2.supabase_url
    supabase_key = variablesid2.supabase_key
    supabase_table = variablesid2.supabase_table
    
    # Inicializar cliente Supabase
    try:
        supabase = create_client(supabase_url, supabase_key)
        print("Conexión con Supabase establecida correctamente")
    except Exception as e:
        print(f"Error al conectar con Supabase: {str(e)}")
        return None
    
    # Obtener la estructura de la tabla para conocer las columnas existentes
    try:
        print("Consultando la estructura de la tabla...")
        # Intenta obtener un registro para ver la estructura
        response = supabase.table(supabase_table).select("*").limit(1).execute()
        
        # Si hay datos, obtenemos las columnas existentes
        if hasattr(response, 'data') and len(response.data) > 0:
            columnas_existentes = set(response.data[0].keys())
            print(f"Columnas existentes en la tabla: {columnas_existentes}")
        else:
            # Si no hay datos, asumimos las columnas más comunes de Aliexpress
            columnas_existentes = {
                'app_sale_price', 'original_price', 'product_detail_url', 
                'second_level_category_name', 'target_sale_price', 
                'second_level_category_id', 'discount', 'product_main_image_url',
                'first_level_category_id', 'target_sale_price_currency',
                'target_app_sale_price_currency', 'tax_rate', 'original_price_currency',
                'shop_url', 'target_original_price_currency', 'product_id',
                'target_original_price', 'product_video_url', 'first_level_category_name',
                'promotion_link', 'sku_id', 'evaluate_rate', 'sale_price',
                'product_title', 'hot_product_commission_rate', 'shop_id',
                'app_sale_price_currency', 'sale_price_currency', 'lastest_volume',
                'target_app_sale_price', 'commission_rate', 'fecha_consulta',
                'small_images'
            }
            print("No hay datos en la tabla. Usando estructura predefinida.")
    except Exception as e:
        # Si hay error al consultar la estructura, asumimos columnas predefinidas
        print(f"Error al consultar la estructura de la tabla: {str(e)}")
        columnas_existentes = {
            'app_sale_price', 'original_price', 'product_detail_url', 
            'second_level_category_name', 'target_sale_price', 
            'second_level_category_id', 'discount', 'product_main_image_url',
            'first_level_category_id', 'target_sale_price_currency',
            'target_app_sale_price_currency', 'tax_rate', 'original_price_currency',
            'shop_url', 'target_original_price_currency', 'product_id',
            'target_original_price', 'product_video_url', 'first_level_category_name',
            'promotion_link', 'sku_id', 'evaluate_rate', 'sale_price',
            'product_title', 'hot_product_commission_rate', 'shop_id',
            'app_sale_price_currency', 'sale_price_currency', 'lastest_volume',
            'target_app_sale_price', 'commission_rate', 'fecha_consulta',
            'small_images'
        }
        print("Usando estructura predefinida de columnas.")
    
    # Obtener fecha y hora actual
    timestamp = datetime.now()
    fecha_consulta = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Iniciando búsqueda para: '{keywords}' en {fecha_consulta}")
    
    # Inicializa el cliente
    client = IopClient(server_url, app_key, app_secret)
    
    # Variables para la paginación
    current_page = 1
    has_next_page = True
    total_productos = 0
    all_products = []
    
    # Bucle para la paginación
    while has_next_page:
        print(f"Obteniendo página {current_page}...")
        
        # Crea una solicitud para la API de productos
        request = IopRequest("aliexpress.affiliate.product.query", http_method="POST")
        
        # Parámetros obligatorios
        request.add_api_param("keywords", keywords)
        request.add_api_param("target_currency", "USD")
        request.add_api_param("target_language", "EN")
        request.add_api_param("page_size", "50")  # Máximo por página
        request.add_api_param("page_no", str(current_page))
        request.add_api_param("tracking_id", "")
        
        # Ejecutar la solicitud
        response = client.execute(request)
        
        # Verificar si tenemos un cuerpo en la respuesta
        if hasattr(response, 'body'):
            try:
                # Extraer los productos correctamente según la estructura de la respuesta
                api_response = response.body.get("aliexpress_affiliate_product_query_response", {})
                resp_result = api_response.get("resp_result", {})
                result = resp_result.get("result", {})
                
                # Verificar si hay productos en esta página
                products_container = result.get("products", {})
                products = products_container.get("product", [])
                
                # Obtener información de paginación
                total_count = int(result.get("total_record_count", 0))
                
                if products:
                    # Guardar todos los productos
                    all_products.extend(products)
                    
                    total_productos += len(products)
                    print(f"Agregados {len(products)} productos de la página {current_page}")
                    
                    # Determinar si hay más páginas
                    if total_count > total_productos:
                        current_page += 1
                        # Pequeña pausa para evitar límites de tasa de la API
                        time.sleep(0.5)
                    else:
                        has_next_page = False
                        print(f"Todos los productos han sido recopilados ({total_productos} de {total_count})")
                else:
                    # No hay más productos
                    has_next_page = False
                    print("No hay más productos disponibles")
            
            except Exception as e:
                print(f"Error al procesar la respuesta de la página {current_page}: {str(e)}")
                has_next_page = False
        else:
            print(f"No se encontró cuerpo en la respuesta de la página {current_page}")
            has_next_page = False
    
    # Convertir a DataFrame y guardar
    if all_products:
        # Crear DataFrame con todos los campos
        df = pd.DataFrame(all_products)
        
        # Agregar columna con fecha y hora de la consulta
        df['fecha_consulta'] = fecha_consulta
        
        # Agregar keyword solo si la columna existe en la tabla de Supabase
        if 'keyword' in columnas_existentes:
            df['keyword'] = keywords
        
        # Manejar campos anidados como promo_code_info
        if 'promo_code_info' in df.columns:
            # Extraer campos anidados
            promo_fields = []
            for idx, promo in df['promo_code_info'].items():
                if isinstance(promo, dict):
                    for key in promo.keys():
                        if f'promo_{key}' not in promo_fields:
                            promo_fields.append(f'promo_{key}')
                            df[f'promo_{key}'] = df['promo_code_info'].apply(
                                lambda x: x.get(key) if isinstance(x, dict) else None
                            )
            
            # Eliminar el campo original anidado
            df.drop('promo_code_info', axis=1, inplace=True)
        
        # Manejar product_small_image_urls que puede ser un diccionario
        if 'product_small_image_urls' in df.columns:
            df['small_images'] = df['product_small_image_urls'].apply(
                lambda x: x.get('string') if isinstance(x, dict) and 'string' in x else 
                          (list(x.values())[0] if isinstance(x, dict) and len(x) > 0 else '[]')
            )
            df.drop('product_small_image_urls', axis=1, inplace=True)
        
        # Guardar en CSV solo si se proporcionó un archivo de salida
        if archivo_salida is not None:
            # Verificar si el archivo ya existe para decidir si incluir encabezados
            file_exists = os.path.isfile(archivo_salida)
            
            # Guardar en CSV (append si ya existe)
            if file_exists:
                df.to_csv(archivo_salida, mode='a', header=False, index=False)
                print(f"Datos agregados al archivo existente: {archivo_salida}")
            else:
                df.to_csv(archivo_salida, index=False)
                print(f"Nuevo archivo creado: {archivo_salida}")
        else:
            print("No se especificó archivo de salida. Omitiendo guardado en CSV.")
        
        # SUBIR A SUPABASE
        print("\nIniciando carga de datos a Supabase...")
        uploaded_count = 0
        
        try:
            # Primero convertimos el DataFrame a una lista de diccionarios
            registros = df.to_dict(orient='records')
            
            # Función para preparar cada registro, incluyendo solo las columnas que existen en la tabla
            def limpiar_registro(reg):
                registro_limpio = {}
                for clave, valor in reg.items():
                    # Solo incluir las columnas que existen en la tabla de Supabase
                    if clave in columnas_existentes:
                        # Convertir NaN a None
                        if isinstance(valor, (float, int)) and pd.isna(valor):
                            registro_limpio[clave] = None
                        # Convertir listas y diccionarios a JSON strings
                        elif isinstance(valor, (list, dict)):
                            registro_limpio[clave] = json.dumps(valor)
                        # Manejar arrays de NumPy
                        elif hasattr(valor, 'dtype'):
                            if pd.isna(valor).all():
                                registro_limpio[clave] = None
                            else:
                                try:
                                    registro_limpio[clave] = json.dumps(valor.tolist())
                                except:
                                    registro_limpio[clave] = str(valor)
                        # Para el resto, usar como está
                        else:
                            registro_limpio[clave] = valor
                return registro_limpio
            
            # Limpiar todos los registros
            registros_limpios = [limpiar_registro(reg) for reg in registros]
            
            # Insertar en lotes
            tamaño_lote = 10  # Ajustar según necesidad
            
            for i in range(0, len(registros_limpios), tamaño_lote):
                lote_actual = registros_limpios[i:i+tamaño_lote]
                
                try:
                    # MÉTODO SIMPLIFICADO QUE FUNCIONA
                    response = supabase.table(supabase_table).insert(lote_actual).execute()
                    
                    # Verificar respuesta
                    if hasattr(response, 'data') and response.data:
                        uploaded_count += len(response.data)
                        print(f"Progreso: {uploaded_count}/{len(df)} registros subidos")
                    else:
                        print(f"Advertencia al insertar lote {i//tamaño_lote + 1}: Sin datos en respuesta")
                
                except Exception as e:
                    print(f"Error al insertar lote {i//tamaño_lote + 1}: {str(e)}")
                    # Intentar uno por uno si falla el lote
                    for j, reg in enumerate(lote_actual):
                        try:
                            response = supabase.table(supabase_table).insert([reg]).execute()
                            uploaded_count += 1
                        except Exception as e2:
                            print(f"  - Error en registro {i+j}: {str(e2)}")
                
                # Pequeña pausa entre lotes
                time.sleep(0.5)
            
            print(f"Carga a Supabase completada. {uploaded_count} de {len(df)} registros subidos.")
            
        except Exception as e:
            print(f"Error general al subir datos a Supabase: {str(e)}")
            import traceback
            traceback.print_exc()
        
        print(f"Búsqueda completada. Se encontraron {total_productos} productos en total.")
        
        # Mostrar información de las columnas guardadas
        print("\nCampos guardados en Supabase:")
        for col in df.columns:
            if col in columnas_existentes:
                print(f"- {col}")
        
        return df
    else:
        print("No se encontraron productos para guardar.")
        return None

# Para uso con cron, puedes configurar los parámetros directamente
keyword = "adidasdadidasar"  # Palabra clave a buscar
archivo_salida = None  # Nombre del archivo para guardar el historial

# Bloque principal para ejecutar el script
if __name__ == "__main__":
    # Ejecutar la función con los parámetros configurados
    print("Iniciando script de búsqueda...")
    resultado = buscar_productos_completos(keyword, archivo_salida)
    print("Script completado.")