from mrjob.job import MRJob
from mrjob.step import MRStep


class MinTemperature(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_location,
                       reducer=self.reducer_min_temperature
                      ),
                MRStep(reducer=self.reducer_sorted_output)
               ]

    def mapper_get_location(self, _, line):
        (sensor_id,location,lat,lon,timestamp,pressure,temperature,humidity) = line.split(',')

        yield location,temperature

    def reducer_min_temperature(self, key, values):

        yield min([float(i) for i in values]), key

    def reducer_sorted_output(self,min,keys):

        for ID in keys:
            yield ID,min



if __name__ == '__main__':
    MinTemperature.run()
