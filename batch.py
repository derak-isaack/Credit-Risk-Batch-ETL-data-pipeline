import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
import pprint
from beam_mysql.connector.io import WriteToMySQL

def main():
    def is_greater_than_threshold(element, threshold):
        return element[1] > threshold

    lines = Pipeline()

    pt = (
        lines
        | beam.io.ReadFromText('data/ds_salaries.csv', skip_header_lines=True)
        | beam.Map(lambda line: line.split(','))
        # | beam.Filter(lambda line: line[4] > '100000')
        #Filter people with a higher risk for defaulting
        | beam.Filter(lambda line:line[9] == '1')
        #Filter persons with salary above 5000000
        | beam.Filter(is_greater_than_threshold, threshold='5000000')
        #Group them by city
        | beam.GroupBy(lambda line: line[5])
        | beam.Map(print)
    )
    pt | WriteToMySQL(
        host="localhost",
        database="batch_streaming",
        table="stream",
        user="root",
        password="@admin#2024*10",
        port=3306,
        batch_size=1000,
    )

    lines.run()
    
if __name__ == '__main__':
    main()
    