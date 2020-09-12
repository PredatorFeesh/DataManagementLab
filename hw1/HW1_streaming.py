import sys

def streamSales(sourceFile):
    with open(sourceFile,'r') as sales:
        for row in sales:
            # Stream the rows
            yield row

def getProductTable(sourceFile): 
    # Store what we want in a hash map dict
    finalTable = {}
    for sale in streamSales(sourceFile):
        t = sale.split(',')
        # 'Customer ID': 0
        # 'Transaction ID': 1
        # 'Date': 2
        # 'Product ID': 3
        # 'Item Cost': 4

        prodId = str(t[3])
        custId = str(t[0])
        
        # Ignore the first line of CSV
        if custId == 'Customer ID': continue

        totalCost = float(t[4])
        
        if prodId in finalTable:
            # We are using the fact that customerIds are gaurenteed to be clustered
            # Thus we can store only the last customer, and not an entire customer set
            if custId != str(finalTable[prodId]['lastCust']):
                finalTable[prodId]['custCount'] += 1
                finalTable[prodId]['lastCust'] = custId
            finalTable[prodId]['totalRev'] += totalCost
        else:
            finalTable[prodId] = { 'custCount': 1, 'totalRev': totalCost, 'lastCust': custId }

    return finalTable

def writeToOutput(table, outputFile):
    # It's fine for us to take an object since Python passes objs via reference
    with open(outputFile, 'w') as ofile:
        ofile.write("Product ID, Customer Count, Total Revenue\n")
        for prod in table:
            # key = product id
            # val = dict of {custCount, totalRev, lastCust}
            
            # Note that we truncate because of the numerical error caused when adding
            ofile.write("{},{},{:.2f}\n".format(prod, table[prod]['custCount'], table[prod]['totalRev']))
            
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("\nUsage")
        print("----------")
        print("python3 HW1_stream.py <DATA FILE> <OUTPUT FILE>\n")
        exit()


    sourceFile = sys.argv[1]
    outFile = sys.argv[2]
    
    table = getProductTable(sourceFile)
    writeToOutput(table, outFile)
        
