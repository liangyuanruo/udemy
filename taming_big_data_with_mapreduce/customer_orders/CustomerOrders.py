from mrjob.job import MRJob
from mrjob.step import MRStep


class MRCustomerAmount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        customerID, itemID, amount = line.split(',')
        yield customerID, float(amount)

    def reducer(self, customerID, amount):
        yield customerID, sum(amount)

if __name__ == '__main__':
    MRCustomerAmount.run()
