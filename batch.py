import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
import pprint
from beam_mysql.connector.io import WriteToMySQL

def main():
    def is_greater_than_threshold(element, threshold):
        return element[4] > threshold

    lines = Pipeline()

    pt = (
        lines
        | beam.io.ReadFromText('data/ds_salaries.csv', skip_header_lines=True)
        | beam.Map(lambda line: line.split(','))
        # | beam.Filter(lambda line: line[4] > '100000')
        | beam.Filter(lambda line:line[0] == '2021')
        | beam.Filter(is_greater_than_threshold, threshold='100000')
        | beam.GroupBy(lambda line: line[1])
        # | beam.Map(lambda group: (group[0], list(group[1])))
        # | beam.io.WriteToText('data/beam_output', file_name_suffix='.csv')
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
    