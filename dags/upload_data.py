import pandas as pd
from faker import Faker
import random
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

# Define product dictionaries
coffee = {
    'Americano': 5.00, 
    'Double Espresso': 5.50, 
    'Cappuccino': 5.00, 
    'Hazelnut Latte': 6.50, 
    'Macchiato': 6.50, 
    'Mocha Cream': 6.00
}
pastry = {
    'Butter Croissant': 3.00, 
    'Cheese Toast': 5.00, 
    'Nutella Roll': 7.00
}
others = {
    'Chai Tea': 4.50, 
    'Green tea': 3.50,
    'Hot Chocolate': 4.00,
}

products = {**coffee, **pastry, **others}

# Define store location dictionaries
location = {
    'Manhattan': 1, 
    'Brooklyn': 2, 
    'Queens': 3, 
    'Bronx': 4,
    'Harlem': 5,
}


@dag(
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['DE_PROJECT4'],
)
def generate_and_load_new_data():

    @task()
    def generate_coffee_shop_data(num_records=1000):
        fake = Faker()
        transactions = []
        product_ids = list(range(1, len(products) + 1))
        product_list = list(products.items())
        
        for _ in range(num_records):
            # Generate transaction details
            transaction_id = fake.uuid4()
            transaction_time = fake.time()
            store_location, store_id = random.choice(list(location.items()))
            payment_type = fake.word(ext_word_list=['Debit', 'Credit'])
            customer_name = fake.first_name()
            
            # Simulate multiple items in one transaction
            num_items = random.randint(1, 5)  # Number of different items in one transaction
            for _ in range(num_items):
                product_index = random.choice(product_ids) - 1
                product_name, product_price = product_list[product_index]

                transaction = {
                    'transaction_id': transaction_id,
                    'transaction_time': transaction_time,
                    'transaction_qty': random.randint(1, 5),
                    'store_id': store_id,
                    'store_location': store_location,
                    'product_id': product_index + 1,  # Use ranged number starting from 1
                    'unit_price': product_price,
                    'product_category': 'Coffee' if product_name in coffee else ('Pastry' if product_name in pastry else 'Other'),
                    'product_name': product_name,
                    'payment_type': payment_type,
                    'customer_name': customer_name
                }
                transactions.append(transaction)
        df = pd.DataFrame(transactions)
        file_path = "/usr/local/airflow/include/coffee_shop_data.csv"
        df.to_csv(file_path, index=False)
        return file_path

    @task()
    def upload_to_landing_container(file_path: str):
        # Fetch container name and access key from Airflow Variables
        container_name = Variable.get("CONTAINER_NAME")
        storage_account_name = Variable.get("STORAGE_ACCOUNT_NAME")
        access_key = Variable.get("ACCESS_KEY")

        # Define ADLS path dynamically
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_adls_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/raw_coffee_shop/coffee_shop_data_{timestamp}.csv"

        # Use the adlfs library to write the file to ADLS
        storage_options = {"account_key": access_key}

        # Read the local file and upload it to ADLS
        data_df = pd.read_csv(file_path)
        data_df.to_csv(data_adls_path, index=False, storage_options=storage_options)

        return f"File uploaded to {data_adls_path}"

    # Task invocation
    file_path = generate_coffee_shop_data()
    upload_to_landing_container(file_path)

# DAG instantiation
load_data_dag = generate_and_load_new_data()
