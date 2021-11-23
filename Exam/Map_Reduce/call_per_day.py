from mrjob.job import MRJob
from mrjob.step import MRStep


class CallDay(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_day_customers,
                       reducer=self.reducer_number_calls
                      ),
                MRStep(reducer=self.reducer_best_day)
               ]

    def mapper_day_customers(self, _, line):
        (CampaignID,
        Cust_ID,
        # Call_Start,
        # Call_End,
        Call_Result,
        Emp_ID, Year, Month, Day, Hour, Target, PLC) = line.split(',')

        yield Day, PLC

    def reducer_number_calls(self, key, values):

        yield sum([float(i) for i in values]), float(key)

    def reducer_best_day(self,sum,keys):

        for ID in keys:
            yield ID,sum



if __name__ == '__main__':
    CallDay.run()
