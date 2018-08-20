import pandas as pd
import numpy as np

import matplotlib as plt
import seaborn as sns

pd.set_option('Display.Precision', 10)

df = pd.read_csv('USA_Housing.csv')
df.head()

# get info on dataframe

df.info()
df.describe()

# We want to predict the price
# Anaconda - sns.distplot(df['Price'])

# corrolation of all columns
# Anaconda - sns.heatmap(df.corr(), annot=True)

# We want to chuck out text info
df.columns

# # Grab our Data from Dataset
# Our X data ( removed Address column)
X = df[['Avg. Area Income', 'Avg. Area House Age', 'Avg. Area Number of Rooms',
       'Avg. Area Number of Bedrooms', 'Area Population', 'Price']]

# Target Variable - what we want to try predict
y = df['Price']

# split our data into training model and testing model

from sklearn.model_selection import train_test_split
# passing data, and specifying test size
# Tuple unpacking
# test size - 40 percent
# random_state - specific set of random splits
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=101)

# Train the model
# Instance of model

from sklearn.linear_model import LinearRegression
lm = LinearRegression()

# train model on training data
lm.fit(X_train, y_train)

# print intersects

print(lm.intercept_)

# print coefficients
print(lm.coef_)


print('Xtrain Columns : {}'.format(X_train.columns))



# Create coefficient 
cdf = pd.DataFrame(lm.coef_, X.columns, columns=['Coeff'])
print('coefficient : {}'.format(cdf))


# # Predictions

predictions = lm.predict(X_test)


print('predictions : {}'.format(predictions))

# Y test to compare
print('y tests : {}'.format(y_test))

# Visualise with scatterplot
#predictions align up correctly with tests, great!

# anaconda - sns.jointplot(predictions, y_test)

# Check out risiduals 
# anaconda - sns.distplot(y_test - predictions)


# # Error Metrics
#from sklearn import metrics

# Absolute error
#print("MAE : {}".format(metrics.mean_absolute_error(y_test, predictions)))

# mean squared error
#print("MSE : {}".format(metrics.mean_squared_error(y_test, predictions)))

# root mean square error
#print("MSquareE :  {}".format(np.sqrt(metrics.mean_squared_error(y_test, predictions)))

