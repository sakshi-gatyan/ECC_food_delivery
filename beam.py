import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery
from datetime import datetime, time

# Command-line argument parser
parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
path_args, pipeline_args = parser.parse_known_args()
inputs_pattern = path_args.input

# Pipeline options
options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)

# Transformation functions
def remove_last_colon(row):
    # Remove the trailing colon from the fifth column of the row		
    cols = row.split(',')		
    item = str(cols[4])			
    if item.endswith(':'):
        cols[4] = item[:-1]		
    return ','.join(cols)		
	
def remove_special_characters(row): 
    # Remove special characters from each column of the row 
    import re
    cols = row.split(',')			
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]', '', col)
        ret = ret + clean_col + ','			
    ret = ret[:-1]						
    return ret

def print_row(row):
    print(row)

# Data processing pipeline
cleaned_data = (
    p
    | beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
    | beam.Map(remove_last_colon)
    | beam.Map(lambda row: row.lower())
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row: row + ',1')		
)

delivered_orders = cleaned_data | 'delivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() == 'delivered')
other_orders = cleaned_data | 'Undelivered Filter' >> beam.Filter(lambda row: row.split(',')[8].lower() != 'delivered')

# Counting operations
total_count = (
    cleaned_data
    | 'count total' >> beam.combiners.Count.Globally() 		
    | 'total map' >> beam.Map(lambda x: 'Total Count:' + str(x))	
    | 'print total' >> beam.Map(print_row)
)

delivered_count = (
    delivered_orders
    | 'count delivered' >> beam.combiners.Count.Globally()
    | 'delivered map' >> beam.Map(lambda x: 'Delivered count:' + str(x))
    | 'print delivered count' >> beam.Map(print_row)
)

undelivered_count = (
    other_orders
    | 'count others' >> beam.combiners.Count.Globally()
    | 'other map' >> beam.Map(lambda x: 'Others count:' + str(x))
    | 'print undelivered' >> beam.Map(print_row)
)

# BigQuery dataset creation
client = bigquery.Client()
dataset_id = "project-id.food_orders_dataset" #hiding project-id for security reasons


try:
    client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)  
    dataset.location = "US"
    dataset.description = "dataset for food orders"
    dataset_ref = client.create_dataset(dataset, exists_ok=True)

# Function to convert CSV string to JSON
def to_json(csv_str):
    fields = csv_str.split(',')
    json_str = {
        "customer_id": int(fields[0]),
        "date": datetime.strptime(fields[1], '%m/%d/%Y').date(),
        "time": datetime.strptime(fields[2], '%H:%M:%S').time(),
        "order_id": fields[3],
        "items": fields[4],
        "amount": float(fields[5]),
        "mode": fields[6],
        "restaurant": fields[7],
        "status": fields[8],
        "ratings": int(fields[9]),
        "feedback": fields[10],
        "delivery_time": int(fields[11]),
        "order_preparation_time": int(fields[12]),
        "premium_member": fields[13],
        "restaurant_location": fields[14],
        "new_col": fields[15]
    }
    return json_str

# BigQuery schema
table_schema = (
    'customer_id:INTEGER,'
    'date:DATE,'
    'time:TIME,'
    'order_id:STRING,'
    'items:STRING,'
    'amount:FLOAT,'
    'mode:STRING,'
    'restaurant:STRING,'
    'status:STRING,'
    'ratings:INTEGER,'
    'feedback:STRING,'
    'delivery_time:INTEGER,'
    'order_preparation_time:INTEGER,'
    'premium_member:STRING,'
    'restaurant_location:STRING,'
    'new_col:STRING'
)


# Write to BigQuery
(delivered_orders
    | 'delivered to json' >> beam.Map(to_json)
    | 'write delivered' >> beam.io.WriteToBigQuery('project-id.food_orders_dataset.delivered_orders',  #Change the target table name-demo3
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        # additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

(other_orders
    | 'others to json' >> beam.Map(to_json)
    | 'write other_orders' >> beam.io.WriteToBigQuery('project-id.food_orders_dataset.other_status_orders',     #Change the target table name-demo4
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        # additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
)

# Running the pipeline
from apache_beam.runners.runner import PipelineState
ret = p.run()
if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error Running beam pipeline')
