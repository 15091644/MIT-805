from mrjob.job import MRJob
from mrjob.step import MRStep


class SkewnessTemperature(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_location,
                       reducer=self.reducer_skewness_temperature
                      ),
                MRStep(reducer=self.reducer_sorted_output)
               ]

    def mapper_get_location(self, _, line):
        (sensor_id,location,lat,lon,timestamp,pressure,temperature,humidity) = line.split(',')

        yield location,temperature

    def reducer_skewness_temperature(self, key, values):

        count = 0

        temps = list()

        for i in values:

            count += 1

            temps.append(float(i))

        sums = float(sum(temps))

        mean = sums/count

        squared_list = list()

        cubed_list = list()

        for temp in temps:

            squared_list.append((temp - mean)**2)

            cubed_list.append((temp - mean)**3)

        std = (sum(squared_list)/count)**(0.5)

        skewness = sum(cubed_list)/ ((count-1)*(std**3))

        yield skewness,key

    def reducer_sorted_output(self,skewness,keys):

        for ID in keys:
            yield ID,round(skewness,2)


if __name__ == '__main__':
    SkewnessTemperature.run()
