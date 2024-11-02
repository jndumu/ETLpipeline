from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
#from airflow.providers.prostgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json



## Defind the DAG
with DAG(
    dag_id='nasa_apod_protgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
)   as dag:
    
    ## step 1: Create the table if it doesn't exist

    @task
    def create_table():
        ## initialize the Postgreshook
        postgres_hook = PostgresHook(postgres_conn_id='mypostgres_connection')

        ## create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            date DATE,
            explanation TEXT,
            url VARCHAR(255),
            media_type VARCHAR(50)
        );
        """
        ## Execute the table creation query
        postgres_hook.run(create_table_query)
        


    ## step2: Extract the nasa api data(APOD) - Astronomy picture of the day[Extract pipeline]
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id="nasa_api",  ## Connection ID Defined in Airflow for NASA API
        endpoint='/planetary/apod',  ## NASA API Endpoint for APOD
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response:response.json(),
            ## USE api key from the connection
    )
    

    ## step3: Transform the data(pick the data that i need to save)
    @task
    def transform_apod_data(response):
        apod_data={
            "title": response.get('title', ""),
            "explanation": response.get('explanation', ""),
            "url": response.get('url', ""),
            "date": response.get('date', ""),
            "media_type": response.get('media_type', ""),
                          
        }
        return apod_data
    
    ## step4: Load the data into the postgres database[Load pipeline]
    @task
    def load_data_to_postgres(apod_data):
        ## initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='mypostgres_connection')
        
        ## Defind the SQL insert Query
        insert_query = """
        INSERT INTO apod_data (title, date, explanation, url, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        ## Execute the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'], 
            apod_data['date'], 
            apod_data['explanation'], 
            apod_data['url'], 
            apod_data['media_type']
            
            ))

    ##step5: Verify the data DBViewer


    ##step6: define the task dependencies
    ## Extract
    create_table() >> extract_apod ## Ensure the table is created befor extraction
    api_response=extract_apod.output

    ## Transform
    transformed_data=transform_apod_data(api_response)
    
    ## Load
    load_data_to_postgres(transformed_data)  ## Task Dependencies

