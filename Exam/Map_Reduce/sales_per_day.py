from mrjob.job import MRJob
from mrjob.step import MRStep


class SalesDay(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_day_outcomes,
                       reducer=self.reducer_number_sales
                      ),
                MRStep(reducer=self.reducer_best_day)
               ]

    def mapper_get_day_outcomes(self, _, line):
        (CampaignID,
        Cust_ID,
        # Call_Start,
        # Call_End,
        Call_Result,
        Emp_ID, Year, Month, Day, Hour, Target, PLC) = line.split(',')

        yield Day,Target

    def reducer_number_sales(self, key, values):

        yield sum([float(i) for i in values]), key

    def reducer_best_day(self,sum,keys):

        for ID in keys:
            yield ID,sum



if __name__ == '__main__':
    SalesDay.run()
