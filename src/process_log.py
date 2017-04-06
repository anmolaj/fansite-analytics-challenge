

#Importing libraries
import pandas as pd
import re


#Writing a function to clean our input log file and grab the required fields
def clean(line):
    """
    input (str) : this is each line contained in the file to be processed
    output : It returns a list of the required fields obtained froma a particular line

    logic: We use various regular expressions to grab each desired element fromt he line
    """

    #We set the regular expression pattern for each required field

    requestMethodPattern='"([A-Z]*)\s.*"'

    resourcePattern='"[A-Z]*\s*(.*).*"'
    requestHttpPattern='"[A-Z]*\s*.*\s+([A-Z]{4}\/.*)"'
    

    fullPattern='^(.*)\s+-\s+-\s+\[(.*)\s+(.*)\]\s+"[A-Z]*\s*.*"\s+(\d.*)?\s+(\d.*|-)$'

    #We will make sure that the line is valid log field
    try:
        requestMethodFull=re.findall(requestMethodPattern,line)
        requestMethod=[requestMethodFull if len(requestMethodFull)!=0 else ['']][0]
        resourceFull=re.findall(resourcePattern,line)

        resource,requestHttp=[resourceFull[0].split() if len(resourceFull[0].split())==2 else (resourceFull[0],'')][0]

        full=re.findall(fullPattern,line)[0]

        return [full[0],full[1],full[2],requestMethod[0],resource,requestHttp,full[3],full[4],line]
    except:
        return


def preProcess(data):

    """
    input (list) : this is a list of values for each field
    output : It returns a dataframe for where each row in the text file has been splitted to required fields

    logic: We preprocess the data, by converting '-' to 0 in bytes ,change format of timestamp and bytes so that it becomes easier to analyse further
    """
    
    headers=["host","timeStamp","timeZone","resourcesMethod","resourceUri","resourceHttp","httpResponse","bytes","original"]
    dfNasa= pd.DataFrame(data,columns=headers)
    dfNasa.loc[dfNasa.bytes== '-', 'bytes'] = 0
    dfNasa["bytes"]=pd.to_numeric(dfNasa["bytes"])
    dfNasa["timeStamp"]=pd.to_datetime(dfNasa["timeStamp"],format='%d/%b/%Y:%H:%M:%S')
    
    return dfNasa


class Features:
    """
    This class contains all features as a function.
    """
    def __init__(self,dfNasa):
        """
        input (dataFrame) : this is the dataframe over which we have to perform analysis
        This initialisation is done to store the values which will be required in other feature methods
        Assuming that there is only one timezone for such kind of activities and hence I am aextracting that and storing it
        """
        self.dfNasa = dfNasa
        self.TZ=dfNasa['timeZone'].unique()[0]
        
    def feature1(self):
        """
        This feature calculates the top 10 most active host/IP addresses that have accessed the site and stores it in hosts.txt
        """

        file=open("./log_output/hosts.txt","w")
        self.dfNasa['host'].value_counts()[:10].reset_index().to_csv(file,sep=',', index=False, header=False)
        file.close()

    def feature2(self):
        """
        This feature calculates the 10 resources that consume the most bandwidth on the site and stores it in resources.txt
        """
        file=open("./log_output/resources.txt","w")
        bwConsumption=pd.DataFrame(self.dfNasa.groupby('resourceUri')['bytes'].agg(['sum'])).reset_index()
        feature2=bwConsumption.sort_values("sum",ascending=False)[:10]
        feature2['resourceUri'].to_csv(file,sep=' ',mode='a', index=False, header=False)
        file.close()
    
    def feature3(self):
        """
        This feature calculates top 10 busiest (or most frequently visited) 60-minute periods and stores it in hours.txt
        """
        file=open("./log_output/hours.txt","w")

        countTS=self.dfNasa.sort_values(['timeStamp']).groupby(['timeStamp'])['timeStamp'].agg(['count'])
        countTS=countTS.reindex(pd.date_range(min(countTS.index),max(countTS.index),freq='s'), fill_value=0)

        # if the length is only less than 10 then no point doing extra stuff
        if len(countTS)<=10:
            countTS.reset_index().to_csv(file,mode='a',sep=',', index=False, header=False)

        #else we will perform a rolling sum starting from the first position
        else:  
            finalCount=countTS[::-1].rolling(window=3600, min_periods=0).sum()[::-1].sort_values(['count'],ascending=False)[:10].reset_index()  
            finalCount['count']=finalCount['count'].astype(int)
           
            finalCount.to_csv(file,mode='a',sep=',', index=False, header=False)
        file.close()
        
    def feature4(self):
        """
        this feature detect patterns of three failed login attempts from the same IP address over 20 seconds so that all further attempts to the site can be blocked for 5 minutes. Log those possible security breaches.
        """
        
        file=open("./log_output/blocked.txt","w")
        dfNasa=self.dfNasa.copy()

        dfNasa.sort_values(["host","timeStamp"],inplace=True)

        #This logic helps to flag (1) those entries which was a 3rd consecutive failed attemp within 20 second
        dfNasa["failed3"]=[1 if http1=='401'and http2=='401' and http3=='401' and x==y and t2-t1<=pd.to_timedelta("20s")                    else 0 for http1,http2,http3,x,y,t1,t2 in zip(dfNasa["httpResponse"],dfNasa["httpResponse"].shift(1),dfNasa["httpResponse"].shift(2),dfNasa["host"],dfNasa["host"].shift(2),dfNasa["timeStamp"],dfNasa["timeStamp"].shift(2))]
        

        #Setting index as host helps to slice the datadrame by hosts quickly
        dfNasa.set_index('host',inplace=True)
        hosts=dfNasa[(dfNasa.failed3==1)].index.unique()
        
        #This loops only goes through hosts which have had 3  consecutive failed attempts
        for ht in hosts:
            #We will now create a dataframe for the current host being considered
            hostSub=dfNasa.loc[ht].reset_index(col_fill='host')

            # we will keep a track of the index where the 3rd consecutive failed attempt was
            ind=hostSub[hostSub.failed3==1].index

            #We will loop through each host for every 5 minutes until all th epoints have been examined
            while(True):
                temp=hostSub.timeStamp[ind[0]]
                hostSub=hostSub[ind[0]+1:]
                indEnd =hostSub[(hostSub.timeStamp<=temp+pd.to_timedelta("300s"))][-1:].index[0]

                tuples=hostSub[hostSub.index.values<=indEnd]["original"]

                for line in tuples:
                    file.write(line)
                if (indEnd==len(hostSub) or sum(hostSub.failed3[indEnd+1:])==0 ):
                    break

                ind=ind[1:]
        file.close()



"""
Now we execute according to our requirements
"""

#We have to make sure that input is correctly taken
try:
    #I use Imap function because it is faster as compared to list comprehension
    dfNasaMain=preProcess(map(clean,open("./log_input/log.txt","r")))
except:
    print "Please check if file is present in /log_input"

#We create an object for feature and then execute for each of the feature required
feature=Features(dfNasaMain.copy())
feature.feature1()
feature.feature2()
feature.feature3()
feature.feature4()

