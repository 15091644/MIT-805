from mrjob.job import MRJob
from mrjob.step import MRStep


class StdTemperature(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_location,
                       reducer=self.reducer_std_temperature
                      ),
                MRStep(reducer=self.reducer_sorted_output)
               ]

    def mapper_get_location(self, _, line):
        (sensor_id,location,lat,lon,timestamp,pressure,temperature,humidity) = line.split(',')

        yield location,temperature

    def reducer_std_temperature(self, key, values):

        count = 0

        temps = list()

        for i in values:

            count += 1

            temps.append(float(i))

        sums = float(sum(temps))

        mean = sums/count

        squared_list = list()

        for temp in temps:

            squared_list.append((temp - mean)**2)

        std = (sum(squared_list)/count)**(0.5)

        yield std,key

    def reducer_sorted_output(self,std,keys):

        for ID in keys:
            yield ID,std


if __name__ == '__main__':
    StdTemperature.run()
