from functions import ProccesorMercaData 

def main():
    processor = ProccesorMercaData()

    processor.fetch_and_store_categories()
    processor.fetch_and_store_products()
    processor.merge_and_storage_data()

if __name__ == "__main__":
    main()
