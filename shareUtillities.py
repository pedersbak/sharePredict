import requests
import json
import pandas as pd
import pickle
import os
import time
def save_obj(obj, name ):
    with open('./bin/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open('./bin/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)
def downloadSymbolHistory(symbol, maxDate='2999-12-31', download=True, type='stock', to_symbol=None):
    # Downloads historik data for symbol.
    # Filters rows newer than maxDate, so that historic data can be simmulated
    # and training the model as of any given historic date can be performed
    url = "https://www.alphavantage.co/query"
    api_key=os.environ["alphavantagetoken"]
    datatype = "json"
    outputsize = "full"
    if type=='stock':
        if symbol==None:
            print("Call: downloadSymbolHistory('DANSKE.CPH', maxDate='2999-12-31', download=True)")
        searchString="Time Series (Daily)" #For searching in json
        function = "TIME_SERIES_DAILY" #For creating right URL
        data = {"function":function,"symbol":symbol,"apikey":api_key,"datatype": datatype,"outputsize" : "full"}
    elif type=='currency':
        from_symbol=symbol
        if from_symbol==None or to_symbol==None:
            print("Call: downloadSymbolHistory(symbol='USD', maxDate='2999-12-31', download=True, type='currency',to_symbol='DKK')")
            print("Missing TO/FROM currencies")
        searchString="Time Series FX (Daily)" #For searching in json
        function = "FX_DAILY" #For creating right URL
        data = {"function":function,"from_symbol":symbol,"to_symbol":to_symbol,"apikey":api_key,"datatype": datatype,"outputsize" : "full"}
    errorCode=0
    if not download:
        myDf=load_obj(symbol)
        print("Note: Loaded persisted time series data")
        return myDf[myDf.index<=maxDate], errorCode
    if type=='stock':
        print("For "+symbol)
    elif type=='currency':
        print("For "+symbol+" to "+to_symbol)
    print("Note: Downloading time series data from "+url)
    page = requests.get(url, params = data)
    response_data = page.json()
    #print(response_data)
    try:
        timeSeriesJson = (response_data[searchString])
    except:
        print("Error - retrying every 60 secs...")
        print(url)
        print(data)
        for i in range(1,4):
            try:
                time.sleep(60)
                print("Retrying...")
                page = requests.get(url, params = data)
                response_data = page.json()
                timeSeriesJson = (response_data[searchString])
                print("Success")
                break
            except Exception as e:
                print(str(e))
                errorCode+=1
                #print("errorCode:"+str(errorCode))
                print("Error - retrying every 60 secs...")
                pass
            if i == 3:
                print("################################################")
                print("Warning: Did not succeed after 3 retrys, exiting")
                print("################################################")
                errorCode=6
                return None, errorCode
        #print(timeSeriesJson)
    #print("Number of datarows downloaded: "+str(len(timeSeriesJson)))
    myDict = {}
    for key, value in timeSeriesJson.items():
        myDict[key] = value.get('4. close')
    myDf = pd.DataFrame.from_dict(myDict, orient='index')    
    myDf.columns = ['Close']
    myDf.index.name = 'Date'
    myDf.sort_index(inplace=True, ascending=True)
    
    lst = []
    prevLst = []
    prevClose = 1
    for index, row in myDf.iterrows():
        if row['Close'] == 0:
            Close = prevClose
        else:
            Close = row['Close']
        if float(row['Close']) != 0:
            prevClose = Close
        lst.append(Close)
        prevLst.append(prevClose)
    #se = pd.Series(lst)
    myDf['Close'] = pd.Series(prevLst).values
    df = pd.DataFrame()
    counter=0
    rel = []
    for index, row in myDf.iterrows():
        if counter==0:
            prev=float(row['Close'])
            #print(float(row['Close']))
        r=(float(row['Close'])-prev)/float(row['Close'])
        rel.append(r)
        counter+=1
        prev=float(row['Close'])
    #print(len(rel))
    se = pd.Series(rel)
    #print(se)
    myDf['Relative'] = se.values
    myDf['dt']=pd.to_datetime(myDf.index,format='%Y-%m-%d')
    save_obj(myDf,symbol)
    return myDf[myDf.index<=maxDate], errorCode
if __name__=="__main__":
	print("*******")
	print("Example: stock call (default)")
	print("a,b = downloadSymbolHistory('DANSKE.CPH', maxDate='2999-12-31', download=True)")
	data,errorCode = downloadSymbolHistory('DANSKE.CPH', maxDate='2999-12-31', download=True)
	print(data[:4])
	print("*******")
	print("Example: currency call")
	print("a,b = downloadSymbolHistory(symbol='USD', maxDate='2999-12-31', download=True, type='currency', to_symbol='DKK')")
	data,errorCoe = downloadSymbolHistory(symbol='USD', maxDate='2999-12-31', download=True, type='currency', to_symbol='DKK')
	print(data[:4])
