from mrjob.job import MRJob
from mrjob.step import MRStep


class MeanTemperature(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_location,
                       reducer=self.reducer_mean_temperature
                      ),
                MRStep(reducer=self.reducer_sorted_output)
               ]

    def mapper_get_location(self, _, line):
        (sensor_id,location,lat,lon,timestamp,pressure,temperature,humidity) = line.split(',')

        yield location,temperature

    def reducer_mean_temperature(self, key, values):

        count = 0

        temps = list()

        for i in values:

            count += 1

            temps.append(float(i))

        sums = float(sum(temps))

        mean = sums/count

        yield mean,key

    def reducer_sorted_output(self,mean,keys):

        for ID in keys:
            yield ID,mean


if __name__ == '__main__':
    MeanTemperature.run()
