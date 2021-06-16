import pandas as pd
import quandl
import numpy as np

#plot pivots de zigzag X serie pivots= peak_valley_pivots(X, 0.01, -0.01)
def plot_pivots(X, pivots):
    plt.xlim(0, len(X))
    plt.ylim(X.min()*0.99, X.max()*1.01)
    plt.plot(np.arange(len(X)), X, 'k:', alpha=0.5)
    plt.plot(np.arange(len(X))[pivots != 0], X[pivots != 0], 'k-')
    plt.scatter(np.arange(len(X))[pivots == 1], X[pivots == 1], color='g')
    plt.scatter(np.arange(len(X))[pivots == -1], X[pivots == -1], color='r')

# recibe dataframe y devuleve el dataframe con el envelope calculado
def calculate_McGinley_envelope(data):
	data['MG_HI']=data['High']
	data['MG_LO']=data['Low']
	for i in range(1, len(data)):
		data.loc[i,'MG_HI'] = data.loc[i-1,'MG_HI'] + (data.loc[i,'High']-data.loc[i-1,'MG_HI']) / (6*pow(data.loc[i,'High']/data.loc[i-1,'MG_HI'], 4))
		data.loc[i,'MG_LO'] = data.loc[i-1,'MG_LO'] + (data.loc[i,'Low']-data.loc[i-1,'MG_LO']) / (6*max(pow(data.loc[i,'Low']/data.loc[i-1,'MG_LO'], 4),0.3))
	data['MG_HI']=data['MG_HI'].shift(4).ffill()
	data['MG_LO']=data['MG_LO'].shift(4).ffill()
	data = data.iloc[4:]
	data.reset_index(inplace=True,drop=True)
	return data

# recibe dataframe y devuleve el dataframe con las zonas Weekly Calculadas
def calculate_weekly_zones(df):
	agg_dict = {'Open': 'first',
          'High': 'max',
          'Low': 'min',
          'Close': 'last',
          'Volume': 'sum'}
	df = df.resample('W-Mon',label="left",closed="left").agg(agg_dict)
	df=calculate_zones(df)
	#df=df.resample('1d').pad()
	return df
	
def calculate_zones(df):
	#Setup Zones
	df['C']=(df['Close']+df['Low']+df['High'])/3
	df['range']=df['High']-df['Low']
	df['D']=df['C']+(df['range']/2)
	df['B']=df['C']-(df['range']/2)
	df['E']=df['C']+(df['range'])
	df['A']=df['C']-(df['range'])
	#Shift zones columns
	#a['x2'] = a.x2.shift(1)
	df['C']=df.C.shift(1)
	df['D']=df.D.shift(1)
	df['B']=df.B.shift(1)
	df['E']=df.E.shift(1)
	df['A']=df.A.shift(1)
	df['range']=df.range.shift(1)
	#SET CLOSE AND OPEN ZONES 	
	conditions=[
		(df['Close']<= df['A']),
		(df['Close']<= df['B']) & (df['Close']> df['A']),
		(df['Close']<= df['C']) & (df['Close']> df['B']),
		(df['Close']<= df['D']) & (df['Close']> df['C']),
		(df['Close']<= df['E']) & (df['Close']> df['D']),
		(df['Close']> df['E'])
	]
	choices=['CL1','CL2','CL3','CL4','CL5','CL6']
	df['CLZONE']=np.select(conditions,choices)
	conditions=[
		(df['Open']<= df['A']),
		(df['Open']<= df['B']) & (df['Open']> df['A']),
		(df['Open']<= df['C']) & (df['Open']> df['B']),
		(df['Open']<= df['D']) & (df['Open']> df['C']),
		(df['Open']<= df['E']) & (df['Open']> df['D']),
		(df['Open']> df['E'])
	]
	choices=['OP1','OP2','OP3','OP4','OP5','OP6']
	df['OPZONE']=np.select(conditions,choices)
	df['CLZONE']=df.CLZONE.shift(1)
	df = df.iloc[2:]
	df['CLOP']=df['CLZONE']+df['OPZONE']
	df.drop(columns=['CLZONE','OPZONE'],inplace = True)
	return df

#Resample Weekly dataframe into daily for mixed probability calculation prep
def df_pad_to_daily(df):
	df=df.resample('1d',label="left",closed="left").pad()
	df.rename(columns={"C": "C_W","A": "A_W","B": "B_W","D": "D_W","E": "E_W",},inplace = True)
	df.rename(columns={'CLOP':'CLOP_W'},inplace=True)
	return df
	
#when padding weekly dataframe, prices stay aggregated from weekly, in order to solve, reapply daily values to current dataframe
def correct_prices_in_weekly(df_fr_W,df):
	df_fr_W["Open"]=df["Open"]
	df_fr_W["High"]=df["High"]
	df_fr_W["Low"]=df["Low"]
	df_fr_W["Close"]=df["Close"]
	df_fr_W["Volume"]=df["Volume"]
	return df_fr_W

#Given a time series with zones returns 2 probability matrixes one for zones and one for bands
def calculate_probs(df):
	# Zones Reached
	conditions=[
		(df['Low']<df['A'])
	]
	choices=[1]
	df['Reach1']=np.select(conditions,choices)
	conditions=[
		(df['High']>=df['B']) & (df['Low']<=df['A']),
		(df['Low']<df['B']) & (df['Low']>=df['A']),
		(df['High']<df['B']) & (df['High']>=df['A'])
	]
	choices=[1,1,1]
	df['Reach2']=np.select(conditions,choices)
	conditions=[
		(df['High']>=df['C']) & (df['Low']<=df['B']),
		(df['Low']<df['C']) & (df['Low']>=df['B']),
		(df['High']<df['C']) & (df['High']>=df['B'])
	]
	choices=[1,1,1]
	df['Reach3']=np.select(conditions,choices)
	conditions=[
		(df['High']>=df['D']) & (df['Low']<=df['C']),
		(df['Low']<df['D']) & (df['Low']>=df['C']),
		(df['High']<df['D']) & (df['High']>=df['C'])
	]
	choices=[1,1,1]
	df['Reach4']=np.select(conditions,choices)
	conditions=[
		(df['High']>=df['E']) & (df['Low']<=df['D']),
		(df['Low']<df['E']) & (df['Low']>=df['D']),
		(df['High']<df['E']) & (df['High']>=df['D'])
	]
	choices=[1,1,1]
	df['Reach5']=np.select(conditions,choices)
	conditions=[
		(df['High']>df['E'])
	]
	choices=[1]
	df['Reach6']=np.select(conditions,choices)
	# Zonas como Resistencia:
	#
	conditions=[
		(df['High']>df['A']) & (df['High']<(df['B']))
	]
	choices=[1]
	df['RES2']=np.select(conditions,choices)
	conditions=[
		(df['High']>df['B']) & (df['High']<(df['C']))
	]
	choices=[1]
	df['RES3']=np.select(conditions,choices)
	conditions=[
		(df['High']>df['C']) & (df['High']<(df['D']))
	]
	choices=[1]
	df['RES4']=np.select(conditions,choices)
	conditions=[
		(df['High']>df['D']) & (df['High']<(df['E']))
	]
	choices=[1]
	df['RES5']=np.select(conditions,choices)
	conditions=[
		(df['High']>df['E']) & (df['Close']<(df['E']))
	]
	choices=[1]
	df['RES6']=np.select(conditions,choices)
	# Zonas como Soporte:
	#
	conditions=[
		(df['Low']<(df['A'])) & (df['Close']>df['A'])
	]
	choices=[1]
	df['SUP1']=np.select(conditions,choices)
	conditions=[
		(df['Low']<(df['B'])) & (df['Low']>df['A'])
	]
	choices=[1]
	df['SUP2']=np.select(conditions,choices)
	conditions=[
		(df['Low']<(df['C'])) & (df['Low']>df['B'])
	]
	choices=[1]
	df['SUP3']=np.select(conditions,choices)
	conditions=[
		(df['Low']<(df['D'])) & (df['Low']>df['C'])
	]
	choices=[1]
	df['SUP4']=np.select(conditions,choices)
	conditions=[
		(df['Low']<(df['E'])) & (df['Low']>df['D'])
	]
	choices=[1]
	df['SUP5']=np.select(conditions,choices)
	# Bandas como zonas de soporte y resistencia
	#
	conditions=[
		(df['Low'] >= (df['C']-(df['range']*0.618))) & (df['Low']<= df['B'])
	]
	choices=[1]
	df['SB1']=np.select(conditions,choices)
	conditions=[
		(df['Low']>=(df['C']-(df['range']*1.382))) & (df['Low']<=df['A']) & (df['Close']>=df['A'])
	]
	choices=[1]
	df['SB2']=np.select(conditions,choices)
	conditions=[
		(df['High']<=(df['C']+(df['range']*0.618))) & (df['High']>=df['D']) 
	]
	choices=[1]
	df['RB1']=np.select(conditions,choices)
	conditions=[
		(df['High']<=(df['C']+(df['range']*1.382))) & (df['High']>=df['E']) & (df['Close']<=df['E']) 
	]
	choices=[1]
	df['RB2']=np.select(conditions,choices)
	#############
	#	Creacion de matriz de probabilidades
	###########
	CLOP_combo=df['CLOP'].sort_values().unique()
	prob=pd.DataFrame(index=CLOP_combo,columns=['count','Reach1','Reach2','Reach3','Reach4','Reach5','Reach6','RES2','RES3','RES4','RES5','RES6','SUP1','SUP2','SUP3','SUP4','SUP5', 'SB1', 'SB2', 'RB1', 'RB2'])
	for i in CLOP_combo:
		data=df[df['CLOP']==i]
		probs_raw=data[['Reach1','Reach2','Reach3','Reach4','Reach5','Reach6','RES2','RES3','RES4','RES5','RES6','SUP1','SUP2','SUP3','SUP4','SUP5','SB1', 'SB2', 'RB1', 'RB2']]
		prob.loc[i,"count"]=data.shape[0]
		for c in probs_raw.columns:
			aux=probs_raw[c].value_counts(normalize=True)     
			if(1 in aux.index):
				prob.loc[i,c]=aux[1]*100.0
			else:
				prob.loc[i,c]=0.0
			if((c in ['RES1','SUP1','SB2']) & (prob.loc[i,'Reach1']!= 0)):
				prob.loc[i,c]=prob.loc[i,c]/prob.loc[i,'Reach1']*100.0
			if((c in ['RES2','SUP2','SB1']) & (prob.loc[i,'Reach2']!= 0)):
				prob.loc[i,c]=prob.loc[i,c]/prob.loc[i,'Reach2']*100.0
			if((c in ['RES3','SUP3']) & (prob.loc[i,'Reach3']!= 0)):
				prob.loc[i,c]=prob.loc[i,c]/prob.loc[i,'Reach3']*100.0
			if((c in ['RES4','SUP4']) & (prob.loc[i,'Reach4']!= 0)):
				prob.loc[i,c]=prob.loc[i,c]/prob.loc[i,'Reach4']*100.0
			if((c in ['RES5','SUP5','RB1']) & (prob.loc[i,'Reach5']!= 0)):
				prob.loc[i,c]=prob.loc[i,c]/prob.loc[i,'Reach5']*100.0
			if((c in ['RES6','SUP6','RB2']) & (prob.loc[i,'Reach6']!= 0)):
				prob.loc[i,c]=prob.loc[i,c]/prob.loc[i,'Reach6']*100.0
	prob.round(2)
	#############
	#	Creacion de matriz de probabilidades Estrictas para bandas
	###########
	prob_strict=pd.DataFrame(index=CLOP_combo,columns=['RB1_s','RB2_s','SB1_s','SB2_s'])
	prob_strict['RB1_s'] = np.divide(prob['RB1']*100, prob['RES5'], out=np.zeros_like(prob['RB1']), where=prob['RES5']!=0)
	prob_strict['RB2_s'] = np.divide(prob['RB2']*100, prob['RES6'], out=np.zeros_like(prob['RB2']), where=prob['RES6']!=0)
	prob_strict['SB1_s'] = np.divide(prob['SB1']*100, prob['SUP2'], out=np.zeros_like(prob['SB1']), where=prob['SUP2']!=0)
	prob_strict['SB2_s'] = np.divide(prob['SB2']*100, prob['SUP1'], out=np.zeros_like(prob['SB2']), where=prob['SUP1']!=0)
	return prob, prob_strict

#Query Quandl for historical data CME asset/instrument
def get_historical_quotes_quandl(symbol,start_date,end_date):
	apiKey="TmwEsGGvzZL-Y9o2o_KZ"
	quandl.ApiConfig.api_key = apiKey
	df = quandl.get('CHRIS/CME_ES1', start_date='2010-01-01', end_date='2021-03-12')
	df.rename(columns={"Last": "Close","Previous Day Open Interest":"Open Interest"},inplace = True)
	df.drop(columns=['Change','Settle'],inplace = True)
	return df
	
#a