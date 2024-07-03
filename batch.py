import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
import pprint
from beam_mysql.connector.io import WriteToMySQL


#Define the main function to ensure efficient execution
def main():
    def is_greater_than_threshold(element, threshold):
        return element[1] > threshold

    lines = Pipeline()

    possible_defaulters = (
        lines
        | "ReadCSVdata" >> beam.io.ReadFromText('data/loan.csv', skip_header_lines=True)
        | "SeparateThevalues" >> beam.Map(lambda line: line.split(','))
        # | beam.Filter(lambda line: line[4] > '100000')
        #Filter people with a higher risk for defaulting
        | "FilterPossibleDefaulters" >> beam.Filter(lambda line:line[12] == '1')
        #Filter persons with salary above 5000000
        | "FilterSalaries" >> beam.Filter(is_greater_than_threshold, threshold='5000000')
        #Group them by city
        | "GroupByCity" >> beam.GroupBy(lambda line: line[9])
        | 'MapToDictionary' >> beam.Map(lambda line: {
            'ID': int(line[0]),
            'Salary': int(line[1]),
            'Age': int(line[2]),
            'Experience': int(line[3]),
            'MaritalStatus': line[4],
            'HousingStatus': line[5],
            'HasCar': line[6],
            'Occupation': line[7],
            'City': line[8],
            'State': line[9],
            'yrs_employed': int(line[10]),
            'house_age': int(line[11]),
            'risk_level': int(line[12])
        })
    )
    possible_defaulters | WriteToMySQL(
        host="localhost",
        database="batch_streaming",
        table="possible_defaulters",
        user="root",
        password="@admin#2024*10",
        port=3306,
        batch_size=1000,
    )
    
    #Define the second transformation pipeline to only include only those who are less likely to default with a risk level of 0.
    less_possible_defaulters = (
        lines
        | "ReadCSVIn text formart" >> beam.io.ReadFromText('data/loan.csv', skip_header_lines=True)
        | "SeparateThevalues" >> beam.Map(lambda line: line.split(','))
        # Filter people with a less risk of possible defaulting
        | "FilterLessPossibleDefaulters" >> beam.Filter(lambda line: line[12] == '0')
        # Group them by city
        | "GroupByCity" >> beam.GroupBy(lambda line: line[9])
        | 'MapToDictionary' >> beam.Map(lambda line: {
            'ID': int(line[0]),
            'Salary': int(line[1]),
            'Age': int(line[2]),
            'Experience': int(line[3]),
            'MaritalStatus': line[4],
            'HousingStatus': line[5],
            'HasCar': line[6],
            'Occupation': line[7],
            'City': line[8],
            'State': line[9],
            'yrs_employed': int(line[10]),
            'house_age': int(line[11]),
            'risk_level': int(line[12])
        })
    )
    
    less_possible_defaulters | WriteToMySQL(
        host="localhost",
        database="batch_streaming",
        table="non_defaulters",
        user="root",
        password="@admin#2024*10",
        port=3306,
        batch_size=1000,
    )

    #Wait untill all the pipeline is exceuted before exit. 
    lines.run().wait_until_finish()
    
    #/Uncomment to run single transformations/
    #lines.run()
    
if __name__ == '__main__':
    main()
    