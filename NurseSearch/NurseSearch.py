#Created on Sunday Aug 28 2016
#
#author: Quinten Geddes

import requests
from lxml import html
import csv

#VARIABLES--
LastNameStart="ZU"

csvpath=r"C:\Users\qgeddes\Documents\Python Scripts\lexisnexis\ZU_Nurses_FirstLicense.csv"
#VARIABLES--

url=r"https://www.ark.org/arsbn/statuswatch/index.php/nurse/search/sort/name/direction/asc/start/"
KeepSearching=True
recordstart=0
MatchingNurses=[]
#Pages contain maximum of 20 records
#cycle through matching records in sets of 20 until no records are found in that set of 20
while KeepSearching==True:
    
    #execute search and return maximum of 20 records staring on recordstart
    r=requests.post(url+str(recordstart), data={"name":LastNameStart})
    
    tree = html.fromstring(r.content)
    
    #pull the rows of the data table
    MatchingRecords=tree.xpath('//table[@class="data_table"]/tr')
    
    print("checking results {0} through {1}: {2} records found".format(recordstart+1,
                                                                       recordstart+20,
                                                                       len(MatchingRecords)))
   
    for row in MatchingRecords:
        #retrieve Nurse name for the row
        name=row.iterchildren().next()
        nametext=name.text_content()
        
        #if nurse last name starts with LastNameStart, add name and hyperlink to MatchingNurses
        if nametext.startswith(LastNameStart):
            NursePageLink=name.iterlinks().next()[2]
            MatchingNurses.append([nametext,NursePageLink])
    
    if len(MatchingRecords)==0: 
        KeepSearching=False        
    recordstart+=20

    
#Using list of Matching Nurses and corresponding links to retrieve license info
#and write it to the csv file indicated above

#setting up csv writer and writing header row
csvfile=open(csvpath,"wb")
writer=csv.writer(csvfile)
writer.writerow(["License Number","Name","License Status","License Type", "Expiration date"])
print("Writing Matching Nurses to CSV file..")
for Nurse in MatchingNurses:
    NurseName=Nurse[0]
    NursePageLink=Nurse[1]
    
    r=requests.get(NursePageLink)
    tree = html.fromstring(r.content)
   
    #pulling required information for first license
    LicNo=tree.xpath('//div[@class="license_table box form"]/h2')[0].text[11:]
    
    #pulling all table rows from license tables, then grabbing the first 11
    #the first 11 rows correspond to the first license only, in case there are multiple
    LicTable=tree.xpath('//div[@class="license_table box form"]/table/tr')[0:11]
    
    LicStatus=LicTable[0].getchildren()[1].text_content()
    
    LicType  =LicTable[1].getchildren()[1].text_content()
    
    ExpDate  =LicTable[5].getchildren()[1].text_content()
    
    #writing out attributes
    writer.writerow([LicNo,NurseName,LicStatus,LicType,ExpDate])
 

writer=0
csvfile.close()
    
