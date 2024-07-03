import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_postgres.io import WriteToPostgres

# Transformation functions
class SplitCSV(beam.DoFn):
    def process(self, element):
        yield element.split(',')

#Filters data to remain with people at higher risk of defaulting
class FilterPossibleDefaulters(beam.DoFn):
    def process(self, element):
        if element[12] == '1':
            yield element

#Filters data only to remain with less possible defaulters        
class FilterLessPossibleDefaulters(beam.DoFn):
    def process(self, element):
        if element[12] == '0':
            yield element

#Filter according to salaries 
class FilterSalaries(beam.DoFn):
    def process(self, element):
        if int(element[1]) > 5000000:
            yield element

#Formart the filtered data to postgres formart
class FormatForPostgres(beam.DoFn):
    def process(self, element):
        yield (
            int(element[0]),   
            int(element[1]),   
            int(element[2]),   
            int(element[3]),   
            element[4],        
            element[5],        
            element[6],       
            element[7],       
            element[8],       
            element[9],       
            int(element[10]), 
            int(element[11]),  
            int(element[12])   
        )

def run_pipeline():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as pipeline:
        possible_defaulters = (
            pipeline
            | "ReadCSVdata" >> beam.io.ReadFromText('data/loan.csv', skip_header_lines=True)
            | "SplitCSV" >> beam.ParDo(SplitCSV())
            | "FilterPossibleDefaulters" >> beam.ParDo(FilterPossibleDefaulters())
            | "FilterSalaries" >> beam.ParDo(FilterSalaries())
            | "FormatForPostgres" >> beam.ParDo(FormatForPostgres()))
            
            
        possible_defaulters | "WriteDefaultersToPostgres" >> WriteToPostgres(
                "host=localhost dbname=data_streams user=postgres password=Admin",
                "insert into possible_defaulters (ID, Salary, Age, Experience, MaritalStatus, HousingStatus, HasCar, Occupation, City, State, yrs_employed, house_age, risk_level) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                batch_size=1000
            )
        
        #Define second filtaration to only include those with less possibility of defaulting.
        lesspossibledefaulters = (
            pipeline
            | "ReadCSVdataNonDefault" >> beam.io.ReadFromText('data/loan.csv', skip_header_lines=True)
            | "SplitCSVNoneDefault" >> beam.ParDo(SplitCSV())
            | "FilterLessPossibleDefaulters" >> beam.ParDo(FilterLessPossibleDefaulters())
            | "FormatForPostgresNonDefaulters" >> beam.ParDo(FormatForPostgres()))
        lesspossibledefaulters | "WriteLessDefaultersToPostgres" >> WriteToPostgres(
                "host=localhost dbname=data_streams user=postgres password=Admin",
                "insert into non_defaulters (ID, Salary, Age, Experience, MaritalStatus, HousingStatus, HasCar, Occupation, City, State, yrs_employed, house_age, risk_level) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                batch_size=1000
            )

if __name__ == '__main__':
    run_pipeline()
