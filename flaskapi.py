from flask import Flask,render_template,url_for,request
import pandas as pd 
from nltk.tokenize import word_tokenize 
from nltk.corpus import stopwords
from sklearn.linear_model import LogisticRegression

import pickle
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib


app = Flask(__name__)

@app.route('/')
def home():
	return render_template('home.html')

@app.route('/predict',methods=['POST'])
def predict():
	df= pd.read_csv("static/news_csv.csv", encoding="latin-1")
	df = df.dropna()
	df.drop(['title'], axis=1, inplace=True)
	# Features and Labels
	df['category'] = df['category'].map({'science':0.0, 'business':1.0,'economics':2.0,'finance':3.0,'tech':4.0,'gaming':5.0, 'entertainment':6.0, 'sport':7.0,'beauty':8.0, 'politics':9.0, 'world':10.0, 'energy':11.0, 'food':12.0, 'travel':13.0})
	X = df['summary']
	y = df['category']
	
	# Extract Feature With CountVectorizer
	cv = CountVectorizer(stop_words="english",min_df = 0.0, max_df = 1.0, ngram_range=(1,2))
	X = cv.fit_transform(X) # Fit the Data
	from sklearn.model_selection import train_test_split
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.10, random_state=42)
	#Naive Bayes Classifier
	from sklearn.naive_bayes import MultinomialNB

	#clf = MultinomialNB()
	clf = LogisticRegression()
	clf.fit(X_train,y_train)
	clf.score(X_test,y_test)
	#Alternative Usage of Saved Model
	# joblib.dump(clf, 'NB_spam_model.pkl')
	# NB_spam_model = open('NB_spam_model.pkl','rb')
	# clf = joblib.load(NB_spam_model)

	if request.method == 'POST':
		message = request.form['message']
		data = [message]
		vect = cv.transform(data).toarray()
		my_prediction = clf.predict(vect)
	return render_template('result.html',prediction = my_prediction)



if __name__ == '__main__':
	app.run(debug=True)