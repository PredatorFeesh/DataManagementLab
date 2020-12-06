import sys
from pyspark import SparkContext

def formatCsv(row):
    if row is None:
        return None
    row = row.split(',')
    
    if row[0] == 'Summons Number':
        return None
    return row

def filterInitData(row):
    if row is None:
        return False
    # If len is right, and not a header, and not empty, and valid year/borough
    return len(row) >= 25 and not row[0] == "Summons Number" and not (
                (row[4] == "" or row[4] == None or row[4] == " ") or
                (row[21] == "" or row[21] == None or row[21] == " ") or
                (row[23] == "" or row[23] == None or row[23] == " ") or
                (row[24] == "" or row[24] == None or row[24] == " ")
            ) and (
                row[4][-4:] in ['2015', '2016', '2017', '2018', '2019']
            ) and (
                row[21] in ['MAN','MH','MN','NEWY','NEW Y','NY','BRONX','BX','BK','K','KING','KINGS','Q','QN','QNS','QU','QUEEN','R','RICHMOND']
            )

def simplifyData(row):
#     return row
    year = int(row[4][-4:])
    borough = row[21]
    houseNum = row[23]
    uStreet = row[24].upper()
    
    # If any of the houseNum is no a digit, remove it, of it no house Number
    if houseNum == '' or houseNum == ' ' or houseNum == None:
        return None # Can't stack this into one OR, because OR checks both and errors
    # Verify that each bit is value. If number for example:
    # 256A is valid. But an address like A is not valid.
    # <number><non-numer> valid given non-number is - or otherwise removed
#     if not all([char.isdigit() or char == '-' for char in houseNum]):
#         return None
    
    ##### NOTE IN THE DATASET FOR MATCHING, TH KILLS THE MATCH. EX: W 8TH ST, TH CAUSES NO MATCHES
    ##### ALSO, OUR CENTERLINE DATASET NEEDS TO BE STRIPPED OF SPACES BECAUSE OF EXTRA SPACES. SAME HERE.
        ### EX: E  8 ST: IS CENTERLINE. BUT HERE, IT'S: E 8TH ST. WON'T MATCH
    newStr = ""
    for part in uStreet.split():
        if part[0].isdigit() and (part[-2:] == 'TH' or part[-2:] == 'ST' or part[-2:] == 'ND'):
            newStr += part[:-2]
        else:
            newStr += part
    uStreet = newStr
    
    ### House number can also be in the form xxxx-xxxxx
    ### So we need to handle that and return as a list
    houseNum = houseNum.split('-')
    
    # Convert each houseNum to int
    for i, num in enumerate(houseNum): # For each split bit
        # If empty or only 1 digit
        if len(num) == 0 or (len(num) == 1 and not houseNum[0].isdigit()):
            return None
        # Make each num in houseNum their new version
        elif len(num) >= 2 and not num[-1].isdigit():
            houseNum[i] = num[:-1]
        else:
            houseNum[i] = num
        
#     houseNum = [int(num) if num[-1].isdigit() else int(num[:-1]) for num in houseNum]
        
    # BOROUGH RETURNED IN FORM SEEN ON BOROCODE IN CENTERLINE
    if borough in ['MAN','MH','MN','NEWY','NEW Y','NY']:
        return (year, 1, houseNum, uStreet)
    elif borough in ['BRONX','BX']:
        return (year, 2, houseNum, uStreet)
    elif borough in ['BK','K','KING','KINGS']:
        return (year, 3, houseNum, uStreet)
    elif borough in ['Q','QN','QNS','QU','QUEEN']:
        return (year, 4, houseNum, uStreet)
    elif borough in ['R','RICHMOND']:
        return (year, 5, houseNum, uStreet)

# Here is our function for matching phsyical ID else return None. Then we'll filter Nones

## THIS ONE MATCHES WITH LESS SEARCHING ON NEWDATA WHICH FOLLOWS NEW FORMAT.
def matchPhysID(item):
    year = item[0]
    boro  = int(item[1])
    
    houseNum = item[2]
    # One known case that fails, human data entry error. Ex: "99-15"
    try:
        houseOdd = int(houseNum[-1]) & 1
    except:
        return None
    
    st = item[3]
    physID = None
    
    # Check A is between B and C; B < A < C
    # This checks each part. Because 123-456 is valid address
    def checkInRange(a, b, c):
        la = len(a)
        lb = len(b)
        lc = len(c)
        # If unequal len not even
        if la != lb or la != lc or la is None or lb is None or lc is None:
            return False
        ai, bi, ci = 0, 0, 0

        for i in range(la):
            # If a part doesn't match, not even
            # If we can't handle, skip
            try:
                ai = int(a[i])
                bi = int(b[i])
                ci = int(c[i])
            except:
                return False
        
            if not ( bi <= ai and ai <= ci ):
                return False
        # Otherwise the houses matched
        return True

    c_data = centerlineB.value
    
    def processPhysId(street_label):
        if street_label not in c_data['st_label']:
            return None
        for label_data in c_data['st_label'][street_label]:
            if int(label_data['borocode']) == boro:
                if houseOdd:
                    if checkInRange(houseNum, label_data['l_low_hn'], label_data['l_high_hn']): # If num is in the range, GOOD
                            return label_data['physicalid']
                else:
                    if checkInRange(houseNum, label_data['r_low_hn'], label_data['r_high_hn']): # If num is in range, GOOD
                            return label_data['physicalid']
    # In order to search, we check if we had a hit for ST
    physID = processPhysId(st)
    if physID is None:
        # Now check full street
        if st in c_data['full_stree']:
            for street in c_data['full_stree'][st]:
                physID = processPhysId(street) # pass street stored on full street
                if physID is not None:
                    break
    # If we are still none, no match
    if physID is None:
        return None
    #(year, 2015, 2016, 2017, 2018, 2019)
    box = [0, 0, 0, 0, 0]
    box[year-2015] = 1
    # This should make it that the year we are currently on has value 1 others 0
    return (int(physID), box)


# Now we want to now aggregate our form:
# physid, 2015, 2016, 2017, 2018, 2019
def aggregListwise(a, b):
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3], a[4] + b[4])

# This should happen before our outer join, so we don't waste time on the 0s
def mapOLS(item):
    # Too small to take cost of broadcast 
    x = [2015.0, 2016.0, 2017.0, 2018.0, 2019.0]
    y = [float(i) for i in item[1]]
    pID = item[0]
    # Our mean
    xmean = sum(x)/5.0
    ymean = sum(y)/5.0
    # now we find (x-xmean)^2 (denom) and our (x-xmean)(y-ymean) (numerator)
    denom = 0.0
    num = 0.0
    xsub = 0.0
    for i in range(5):
        xsub = x[i] - xmean
        denom += xsub**2
        num += xsub * (y[i] - ymean)
    final = num / denom
    # now we return the final, merge it onto the years
    return (pID, (item[1][0],item[1][1],item[1][2],item[1][3],item[1][4], final) )

def mapCSV(item):
    # Was empty -> ('100018', (0, None)),
    # Had data -> ('100019', (0, (34, 0, 0, 0, 0)))
    toOut = []
    if item[1][0] is None and item[1][1] is None:
        toOut = (int(item[0]), (0,0,0,0,0,0))
    else:
        toOut = (int(item[0]), item[1][1])
    return '{}, {}, {}, {}, {}, {}, {}'.format(toOut[0], toOut[1][0], toOut[1][1], toOut[1][2], toOut[1][3], toOut[1][4], toOut[1][5])

if __name__ == '__main__':
    # USAGE:
    # spark-sumit <settings> --files centerline.json PIDs.txt BDM_final_local_azayev.py <output>
    import json
    
    out = sys.argv[1]
    
    sc = SparkContext.getOrCreate()

    data = None
    with open('centerline.json, 'r') as f:
        data = json.loads(f.read())
 
    centerlineB = sc.broadcast(data)
    
    # Get all of our RDDS in
    PIDRDD = sc.textFile('PIDs.txt').flatMap(lambda x: x.split(',')).map(lambda x: (int(x), None))

    rdd = sc.textFile('../../DATA/Parking_Violations_Issued_-_Fiscal_Year_*.csv').map(formatCsv)
    
    rdd = rdd \
    .filter(filterInitData) \
    .map(simplifyData) \
    .filter(lambda item: item is not None and item[0] is not None and item[1] is not None and item[3] is not None) \
    .map(matchPhysID).filter(lambda x: x is not None and x[0] is not None) \
    .reduceByKey(aggregListwise) \
    .map(mapOLS)
              
    PIDRDD.fullOuterJoin(rdd) \
    .sortByKey() \
    .map(mapCSV) \
    .saveAsTextFile(out)

    
    
    