from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

# ORDER OF OUTPUT:
# product (name should be written in all lowercase)
# year
# total number of complaints received for that product and year
# total number of companies receiving at least one complaint for that product and year
# highest percentage (rounded to the nearest whole number) of total complaints filed against one company for that product and year.

class CSVProtol(object):
    # This is our final write protocol, since we need the output as CSV.
    # It was stated in the assignment that we can ignore the head of the CSV.
    def write(self, _, value):
        ret = ''
        for word in value:
            word = str(word)
            # We want to append the word. There is a condition where multiple products are
            # appended together as one, so we need to put "" around it. We do so via checking
            # for a comma already present in the word.
            ret+= (word+',') if ',' not in word else ('"'+word+'",')
        # We have to return bytes, so we convert out utf-8 encoded str to byte array
        # We also don't take the last element of the string, as it is a comma
        return bytearray(ret[0:len(ret)-1], encoding='utf-8') # Remove last comma


class ProcessConsumerComplaints(MRJob):

    # Specify that we want the output as CSV (Via the CSV Protocol written here)
    # We need a special protocol because of the special case that there can be multiple products considered as one product.
    # I.E: financial services, consumer loan <= can be considered one product. But, CSV will see this as two seperate columns.
    # We need to wrap it in quotes.
    OUTPUT_PROTOCOL = CSVProtol

    SORT_VALUES = True
    
    # First we need to process our CSV and there is no default way of doing so
    # so let's do this via entire file processing (since multilines can happen we can't rely on
    # reading a line at a time)
    def mapper_raw(self, fpath, furi):
        with open(fpath, 'r') as csvfile:
            # We will use the dict reader which seems to handle the multifile csv
            reader = csv.DictReader(csvfile)
            for row in reader:
                # And we yield to stay consistent with mapping when reading out file
                # yields: (product, year), (company)
                yield ( (row['Product'].lower(), row['Date sent to company'][0:4]), (row['Company']) )
    
    def reducer_prodDate_numCompaniesComplaints(self, prodDate, comp):
        # (product, year)
        companies = list(comp)
        ucompanies = list(set(companies))

        # By taking the total number of companies in this list, we can get our total number of complaints.
        numComplaints = len(companies)
        # By taking the number of unique companies, we get the number of compaines complaints were against.
        uNumCompanies = len(ucompanies)
        
        # yields: (product, year), (number of complaints, number of companies)
        yield (prodDate, (companies, numComplaints, uNumCompanies) )

    def map_pyc(self, prodDate, compData):
        # We want to combine product year company into one key
        # This does introduce redundent data, but also allows us to get the total number of
        # unique complaints per company.

        # The plan here is to use the combiner in order to get the total number of complaints for company/prod/year
        # thus grouping this redundant data over N times, given we have N compltains against a 
        # given (company, product, year).
        listCompData = list(compData)
        for compName in listCompData[0]: # For each company
            # yields: (product, year, company), (number of compltaints, number of companies)
            yield ( list(prodDate) + [compName], listCompData[1:3])
    
    def reduce_percentageComplaints(self, pyc, compData):
        # Now we want to find how many pieces of data we atually get
        # This will because of the combine step between map and reduce, get  us the number
        # of complaints against the given company in the year and product. We can then divide
        # this by the total complaints against this year, product combo in the data.
        pyc, compData = list(pyc), list(compData)
        numComp = len(compData)
        percentage = numComp / compData[0][0] # All compData is the same, so we can look at the very first one
        # (product, year), (percent complaint against company, total prod/year complaints, total #companies complained prod/year)
        yield ((pyc[0], pyc[1]), (percentage, compData[0][0], compData[0][1]))

    def reduce_maxComplaints(self, py, compData):
        # Now we want to extract out all the percentages so we can max.
        # Note that totalRep and totalComp are all the same.
        percent, totalRep, totalComp = zip(*list(compData))

        # Because totalRep and totalComp are all the same value, we just take the first.
        yield (None, (py[0], py[1], totalRep[0], totalComp[0], round(max(percent) * 100)))

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw),
            MRStep(reducer=self.reducer_prodDate_numCompaniesComplaints),
            MRStep(mapper=self.map_pyc, reducer=self.reduce_percentageComplaints),
            MRStep(reducer=self.reduce_maxComplaints)
        ]


if __name__ == "__main__":
    ProcessConsumerComplaints.run()