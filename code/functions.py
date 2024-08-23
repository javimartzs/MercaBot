import requests 
import pandas as pd 
from datetime import datetime  
import time 
import os 


class ProccesorMercaData:

    def fetch_and_store_categories(self):
        url = 'https://tienda.mercadona.es/api/categories/?lang=es&wh=mad1'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            categories = []

            for result in data.get('results', []):
                agregate_id = result.get('id')
                agregate_name = result.get('name')
                
                for cat in result.get('categories', []):
                    category_id = cat.get('id')
                    category_name = cat.get('name')

                    categories.append({
                        'agregate_id': agregate_id, 
                        'agregate_name': agregate_name, 
                        'category_id': category_id, 
                        'category_name': category_name
                    })
            
            df = pd.DataFrame(categories)
            df.to_csv('data/categories.csv', index=False)
            print('Categorias almacenadas correctamente')

        else:
            print(f'Error en la solicitud GET: {response.status_code}')


    def fetch_and_store_products(self):
        
        df = pd.read_csv('data/categories.csv')
        categories_id = df['category_id'].unique().tolist()

        products = []
        date = datetime.now().strftime('%d%m%Y')

        for category in categories_id:
            url = f"https://tienda.mercadona.es/api/categories/{category}/?lang=es&wh=mad1"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()

                for cat in data.get('categories', []):
                    for product in cat.get('products', []):
                        
                        product_id = product['id']
                        product_slug = product['slug']
                        product_name = product['display_name']
                        packaging = product['packaging']
                        share_url = product['share_url']
                        thumbnail = product['thumbnail']

                        iva = product['price_instructions']['iva']
                        unit_size = product['price_instructions']['unit_size']
                        price = product['price_instructions']['unit_price']
                        reference_format = product['price_instructions']['reference_format']
                        reference_price = product['price_instructions']['reference_price']

                        products.append({
                            'date': date, 
                            'category_id': category,
                            'product_id': product_id, 
                            'product_slug': product_slug, 
                            'product_name': product_name, 
                            'packaging': packaging, 
                            'share_url': share_url, 
                            'thumbnail': thumbnail, 
                            'iva': iva, 
                            'unit_size': unit_size, 
                            'price': price, 
                            'reference_format': reference_format, 
                            'reference_price': reference_price
                        })
                    
            print(f"Categoria {category} añadida a productos")
            time.sleep(1)

        else:
            print(f'Error en la solicitud GET: {response.status_code}')

        df = pd.DataFrame(products)
        df.to_csv('data/products.csv', index=False)
        print('Productos añadidos correctamente')



    def merge_and_storage_data(self):
        
        date = datetime.now().strftime('%d%m%Y')

        categories = pd.read_csv('data/categories.csv')
        products = pd.read_csv('data/products.csv')

        df = pd.merge(products, categories, on='category_id', how='inner')
        df.to_parquet(f'data/precios_{date}.parquet')
        try:
            os.remove('data/categories.csv')
            os.remove('data/products.csv')

        except FileNotFoundError as e:
            print(f"Error al intentar eliminar los archivos: {e}")
        
        print(f"Fichero de {date} almacenado")
        

