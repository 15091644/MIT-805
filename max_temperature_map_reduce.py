from mrjob.job import MRJob
from mrjob.step import MRStep


class MaxTemperature(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_location,
                       reducer=self.reducer_max_temperature
                      ),
                MRStep(reducer=self.reducer_sorted_output)
               ]

    def mapper_get_location(self, _, line):
        (sensor_id,location,lat,lon,timestamp,pressure,temperature,humidity) = line.split(',')

        yield location,temperature

    def reducer_max_temperature(self, key, values):

        yield max([float(i) for i in values]), key

    def reducer_sorted_output(self,max,keys):

        for ID in keys:
            yield ID,max



if __name__ == '__main__':
    MaxTemperature.run()
