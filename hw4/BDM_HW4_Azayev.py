from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys


def reduce_complaints(x) :
    prodDate, comp = x[0], x[1]
    companies = list(comp)
    ucompanies = list(set(companies))
    # By taking the total number of companies in this list, we can get our total number of complaints.
    numComplaints = len(companies)
    # By taking the number of unique companies, we get the number of compaines complaints were against.
    uNumCompanies = len(ucompanies)
    # yields: (product, year), (number of complaints, number of companies)
    yield (prodDate, (companies, numComplaints, uNumCompanies) )

def map_pyc(x):
    prodDate, compData = x[0], x[1]
    # We want to combine product year company into one key
    # This does introduce redundent data, but also allows us to get the total number of
    # unique complaints per company.

    # The plan here is to use the combiner in order to get the total number of complaints for company/prod/year
    # thus grouping this redundant data over N times, given we have N compltains against a 
    # given (company, product, year).
    listCompData = list(compData)
    for compName in listCompData[0]: # For each company
        # yields: (product, year, company), (number of compltaints, number of companies)
        yield ((prodDate[0], prodDate[1], compName), (listCompData[1:3])) 

        
def reduce_percentageComplaints(x):
    pyc, compData = x[0], x[1]
    # Now we want to find how many pieces of data we atually get
    # This will because of the combine step between map and reduce, get  us the number
    # of complaints against the given company in the year and product. We can then divide
    # this by the total complaints against this year, product combo in the data.
    pyc, compData = list(pyc), list(compData)
    numComp = len(compData)
    percentage = numComp / compData[0][0] # All compData is the same, so we can look at the very first one
    # (product, year), (percent complaint against company, total prod/year complaints, total #companies complained prod/year)
    yield ((pyc[0], pyc[1]), (percentage, compData[0][0], compData[0][1]))

def reduce_maxComplaints(x):
    py, compData = x[0], x[1]
    # Now we want to extract out all the percentages so we can max.
    # Note that totalRep and totalComp are all the same.
    percent, totalRep, totalComp = zip(*list(compData))
    roun = round(max(percent) * 100)

    # Because totalRep and totalComp are all the same value, we just take the first.
    yield py[0], py[1], totalRep[0], totalComp[0], roun

    


if __name__ == '__main__':

    sc = SparkContext.getOrCreate()
    file = sys.argv[1] if len(sys.argv)>1 else 'complaints_small.csv'
    output = sys.argv[2] if len(sys.argv)>2 else 'output'


# Create Spark session
    spark = SparkSession.builder \
        .master('') \
        .appName('') \
        .getOrCreate()

# Convert list to data frame
    df = spark.read.format('csv') \
                    .option('header',True) \
                    .option('multiLine', True) \
                    .option("escape", "\"") \
                    .csv(file)

    rdd = df.rdd.map(tuple)
# yields: (product, year), (company)
    rdd = rdd \
    .filter(lambda x: x is not None and x[13] is not None and x[7] is not None and x[1] is not None) \
    .map(lambda x: ((x[1], x[13][0:4]), x[7])).groupByKey() \
    .flatMap(reduce_complaints) \
    .flatMap(map_pyc).groupByKey() \
    .flatMap(reduce_percentageComplaints).groupByKey() \
    .flatMap(reduce_maxComplaints) \
    .saveAsTextFile(output)


