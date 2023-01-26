# Correlation Error Matrix : Example
# Linkage Variables: Sex, Full Name, DoB
# Attribute Variables: Ethnicity, Decade of Death 
# Value of Attribute Variable may associate with Linkage Variable Errors
# Error of Linkage Variable may associate with Linkage Variable Errors
# A Node in a partial Correlation Network reoreesents a variable, and each edge represents that 2 variables are not indepedent [associated] after conditioning on all variables in the data set.

# Visualising relationship between Linkage & Attribute Errors (regardless of value or error) help retain such dependencies in the data corruption process
# for example, given relationship between Sex and Full Name, corruption of Sex should mean there is a higher likelihood Full Name is also corrupted.
# The Edge Weights between Variables indicate how partially correlated variables are, controlling for all other nodes in the network. 
# The ISING model can be used to compute probabilities of each combination of errors, which are used to describe the error matrix pattern.
# Threshold Functions can be parametized with pre-set probabilities for each node to be erroneous [-1 to 1]


# Example: synthesise Error Matrix
# Represent -1 as "associated with error", +1 as "Not associated with Error"
# For Sex,  Assume Error (1) occurs 50 out of 1000 times
# For Full Name, Assume Error (1) occurs 300 out of 1000 times
# For DoB, Assume Error (1) occurs 100 out of 1000 times
# For Ethnicity, Assume Error (1) occurs 250 out of 1000 times, where if Full name is error
# For Decade of Death, Assume Error (1) occurs 50 out of 1000 times.
 
import numpy as np
import pandas as pd
#Sex
N = 1000
K = 50
arr = np.array([-1] * K + [1] * (N-K))
np.random.shuffle(arr)
list_sex = arr.tolist()

#Full Name
N = 1000
K = 300
arr_FN = np.array([-1] * K + [1] * (N-K))
np.random.shuffle(arr_FN)
list_fn = arr_FN.tolist()
#DoB
N = 1000
K = 100
arr_DoB = np.array([-1] * K + [1] * (N-K))
np.random.shuffle(arr_DoB)
list_dob = arr_DoB.tolist()
#Ethnicity
N = 1000
K = 250
arr_Ethnicity = np.array([-1] * K + [1] * (N-K))
np.random.shuffle(arr_Ethnicity)
list_ethnicity = arr_Ethnicity.tolist()
#DoD
N = 1000
K = 50
arr_DoD = np.array([-1] * K + [1] * (N-K))
np.random.shuffle(arr_DoD)
list_dod = arr_DoD.tolist()

error_matrix = [list_sex, list_fn, list_dob, list_ethnicity, list_dod]
# print(error_matrix)

# error_matrix = np.array([[list_sex], [list_fn], [list_dob], [list_ethnicity], [list_dod]])
# np.corrcoef(error_matrix)

""" 
Panda Data Frame
data = {
    'Sex': [list_sex],
    'Full Name': [list_fn],
    'DoB': [list_dob],
    'Ethnicity': [list_ethnicity],
    'DoD': [list_dod]
}
df = pd.DataFrame(data, columns = ['Sex', 'Full Name', 'DoB', 'Ethnicity', 'DoD'])
df.head(20)

Simple Correlation

corr_matrix = df.corr()
print(corr_matrix) 
"""

""" 
import bnlearn as bn
df = error_matrix
model = bn.structure_learning.fit(df)
model = bn.independence_test(model, df)
G = bn.plot(model)
"""

# Partial Correlation Calcuation, uses pandas df
# illustrative example

Data = {
    'Sex': list_sex,
    'Full Name': list_fn,
    'DoB': list_dob,
    'Ethnicity': list_ethnicity,
    'DoD': list_dod
}

df = pd.DataFrame(Data, columns = ['Sex', 'Full Name', 'DoB', 'Ethnicity', 'DoD'])
df.head()

import pingouin as pg

# Pair-wise Partial Correlation
# pg.partial_corr(data = df, x = 'Sex', y = 'Full Name', covar = ['DoB', 'Ethnicity', 'DoD'], method='pearson') 

# partial correlation matrix
Data.pcorr().round(3)
