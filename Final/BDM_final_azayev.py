import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def filterInitData(row):
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
def matchPhysID(item):
    year = item[0]
    boro  = item[1]
    
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
    
    for c_item in c_data['features']:
        # Street in full_stree
        # Street in street_label
        # Borough code matches
        if (st == c_item['properties']['full_stree'] or
        st == c_item['properties']['st_label']) and boro == int(c_item['properties']['borocode']): 
            # If we are here, confirms that street exists in the right boro
            # Now check whether house is right
            if houseOdd: # If odd house check L
                if checkInRange(houseNum, c_item['properties']['l_low_hn'], c_item['properties']['l_high_hn']): # If num is in the range, GOOD
                    physID = c_item['properties']['physicalid']
                    break
            else: # If even check R
                if checkInRange(houseNum, c_item['properties']['r_low_hn'], c_item['properties']['r_high_hn']): # If num is in range, GOOD
                    physID = c_item['properties']['physicalid']
                    break
    return (physID, year, boro)




if __name__ == '__main__':
    import json
    
    geofile, out = sys.argv[1], sys.argv[2]
    
    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.format('csv') \
                    .option('header',True) \
                    .option('sep', ',') \
                    .option('multiLine', True) \
                    .load('/data/share/bdm/nyc_parking_violations/*.csv')

    rdd = df.rdd.map(tuple)
    
    rdd = rdd.filter(filterInitData).map(simplifyData).filter(lambda item: item is not None and item[0] is not None and item[1] is not None and item[3] is not None)
   
    data = None
    with open(geofile, 'r') as f:
        data = json.loads(f.read())
 
    # First broadcast
    centerlineB = sc.broadcast(data)
    
    rdd.map(matchPhysID).filter(lambda x: x is not None and x[0] is not None).saveAsTextFile(output)
    
    
    