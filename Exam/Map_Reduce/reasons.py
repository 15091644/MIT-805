from mrjob.job import MRJob
from mrjob.step import MRStep


class Reasons(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_reasons,
                       reducer=self.reducer_number_reasons
                      ),
                MRStep(reducer=self.reducer_most_reasons)
               ]

    def mapper_get_reasons(self, _, line):
        (CampaignID,
        Cust_ID,
        # Call_Start,
        # Call_End,
        Call_Result,
        Emp_ID, Year, Month, Day, Hour, Target, PLC) = line.split(',')

        yield Call_Result,PLC

    def reducer_number_reasons(self, key, values):

        yield sum([float(i) for i in values]), key

    def reducer_most_reasons(self,sum,keys):

        for ID in keys:
            yield ID,sum



if __name__ == '__main__':
    Reasons.run()
