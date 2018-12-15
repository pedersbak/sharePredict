import requests
import json
import pandas as pd
import pickle
def save_obj(obj, name ):
    with open('./bin/'+ name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open('./bin/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)






def downloadSymbolHistory(symbol, maxDate='2999-12-31', download=True):
    # Downloads historik data for symbol.
    # Filters rows newer than maxDate, so that historic data can be simmulated
    # and training the model as of any given historic date can be performed
    errorCode=0
    if not download:
        myDf=load_obj(symbol)
        print("Note: Loaded persisted time series data")
        return myDf[myDf.index<=maxDate], errorCode

    url = "https://www.alphavantage.co/query"
    function = "TIME_SERIES_DAILY"
    api_key = "JS1OH18PC49XLGCG"   
    datatype = "json"
    outputsize = "full"

    data = { "function"   : function, 
             "symbol"     : symbol, 
             "apikey"     : api_key ,
             "datatype"   : datatype,
             "outputsize" : 'full'} 
    print("For "+symbol)
    print("Note: Downloading time series data from "+url)
    page = requests.get(url, params = data)
    response_data = page.json()
    #print(response_data)
    try:
        timeSeriesJson = (response_data["Time Series (Daily)"])
    except:
        print("Error - retrying every 60 secs...")
        for i in range(1,4):
            try:
                time.sleep(60)
                print("Retrying...")
                page = requests.get(url, params = data)
                response_data = page.json()
                timeSeriesJson = (response_data["Time Series (Daily)"])
                print("Success")
                break
            except:
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
    #prevClose=myDf.iloc[0,0].Close
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
    #myDf['Prev']  = pd.Series(prevLst).values
    
    #print(se)
    
    #print(myDf)    

    
    
    
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
    save_obj(myDf,symbol)
    return myDf[myDf.index<=maxDate], errorCode

