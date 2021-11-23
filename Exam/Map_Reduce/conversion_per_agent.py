from mrjob.job import MRJob
from mrjob.step import MRStep


class SalesAgent(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_agent_outcomes,
                       reducer=self.reducer_number_sales
                      ),
                MRStep(reducer=self.reducer_best_agent)
               ]

    def mapper_get_agent_outcomes(self, _, line):
        (CampaignID,
        Cust_ID,
        # Call_Start,
        # Call_End,
        Call_Result,
        Emp_ID, Year, Month, Day, Hour, Target, PLC) = line.split(',')

        yield Emp_ID,Target

    def reducer_number_sales(self, key, values):

        yield sum([float(i) for i in values]), key

    def reducer_best_agent(self,sum,keys):

        for ID in keys:
            yield ID,sum



if __name__ == '__main__':
    SalesAgent.run()
