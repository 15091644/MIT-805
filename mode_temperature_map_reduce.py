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

        index = count // 2

        if count % 2:

            yield sorted(temps)[index], key

        else:

            yield sum(sorted(temps)[index - 1:index + 1]) / 2, key

    def reducer_sorted_output(self,mode,keys):

        for ID in keys:
            yield ID,mode


if __name__ == '__main__':
    StdTemperature.run()
