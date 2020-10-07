from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

# ORDER OF OUTPUT:
# product (name should be written in all lowercase)
# year
# total number of complaints received for that product and year
# total number of companies receiving at least one complaint for that product and year
# highest percentage (rounded to the nearest whole number) of total complaints filed against one company for that product and year.

# This output summary hints at what we want as a key (product, year) and value (company)
# We only care about numbers

class ProcessConsumerComplaints(MRJob):

    # First we need to process our CSV and there is no default way of doing so
    # so let's do this via entire file processing (since multilines can happen we can't rely on
    # reading a line at a time)
    def mapper_raw(self, fpath, furi):
        with open(fpath, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield ( (row['Product'], row['Date sent to company']), (row['Company']) )
    
    def reducer_prodDate_numComplaints(self, prodDate, comp):
        companies = list(comp)
        numComplaints = len(companies)
        yield (prodDate, (companies, numComplaints) )
    
    def reducer_prodDate_numCompanies(self, prodDate, companiesComplaints):
        companiesComplaints = list(companiesComplaints)[0]
        companies, numComplaints = companiesComplaints
        ucompanies = set(companies)
        numCompanies = len(ucompanies)
        yield (prodDate, (companies, numComplaints, numCompanies) )

    
    # Now that we have our Product, Date and Company, that's all we need to get:
    # Total # complaints per company/year
    # Total # companies per prod/year
    # Highest % of total complaints against company for product/year,

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw),
            MRStep(reducer=self.reducer_prodDate_numComplaints),
            MRStep(reducer=self.reducer_prodDate_numCompanies)
        ]


if __name__ == "__main__":
    ProcessConsumerComplaints.run()