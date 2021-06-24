import pandas as pd
import seaborn as sns
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
np.seterr(divide='ignore', invalid='ignore')

def model_linear(df,y,cdd,hdd):
    is_cdd = lambda mean: mean - cdd if mean>cdd else 0
    is_hdd = lambda mean: hdd - mean if mean<hdd else 0
    df["HDD"] = df["Mean"].apply(is_hdd)
    df["CDD"] = df["Mean"].apply(is_cdd)        
    analysis_df = pd.DataFrame()
    analysis_df["CDD"] = df.groupby(["Year","Month"]).CDD.sum()
#     analysis_df["CDD"] = df.groupby(["Year","Month"]).CDD.sum().values
    analysis_df["HDD"] = df.groupby(["Year","Month"]).HDD.sum().values
    x_train = analysis_df
    data_points = len(y)
    k = len(analysis_df.columns)
    dof = data_points-1-k
    y_train  = y
    model = LinearRegression()
    model.fit(x_train,y_train)
    coefs = model.coef_
    intercept = model.intercept_
    prediction = []
    residuals = []
    avResiduals = []
    avModel = []
    variation = []
    stdRes = []
    total_model_prediction = 0
    residuals_df = pd.DataFrame()
    for j in range(len(analysis_df["CDD"])):
        pred = analysis_df["CDD"][j]*coefs[0] + analysis_df["HDD"][j]*coefs[1] + intercept
        total_model_prediction += pred
        prediction.append(pred)

        residual = y_train[j] - pred
        residuals.append(residual)
    
        avRes = residual / data_points
        avResiduals.append(avRes)
        
    for j in range(len(analysis_df["CDD"])):
        av_Model = total_model_prediction/data_points
        avModel.append(av_Model)
        var = avResiduals[j] + av_Model
        variation.append(var)
    
        
    residuals_df["Residuals"] = residuals
    residuals_df["Av. Residuals"] = avResiduals
    residuals_df["Av. Model"] = avModel
    residuals_df["Variation"] = variation
    
    residuals_std= residuals_df["Residuals"].std()
    for i in range(len(prediction)):
        standarized_res = residuals_df["Av. Residuals"][i]/residuals_std
        stdRes.append(standarized_res)
    residuals_df["Standarized Residuals"] = stdRes
    r2 = r2_score(y, prediction)
    adj_r2 = 1 - (1 -r2)*(data_points-1)/dof
    data_matrix = [y.values,analysis_df["CDD"].values,analysis_df["HDD"].values]
    coef_correl = np.corrcoef(data_matrix)
    r12 = coef_correl[2][1]
    x1_avg = analysis_df["CDD"].mean()
    x2_avg = analysis_df["HDD"].mean()
    sum_sq_x1 = 0
    sum_sq_x2 = 0
    for i in range(len(analysis_df)):
        sum_sq_x1 += (analysis_df["CDD"][i] - x1_avg)**2
        sum_sq_x2 += (analysis_df["HDD"][i] - x2_avg)**2    
    ssres = 0
    for i in range(len(residuals_df["Residuals"])):
        ssres += (residuals_df["Residuals"][i])**2
    sy12 = ssres / dof
    see = sy12**0.5
    sb1 = (sy12/(sum_sq_x1*(1-r12**2)))**0.5
    sb2 = (sy12/(sum_sq_x2*(1-r12**2)))**0.5
    
    t1 = coefs[0]/sb1
    t2 =  coefs[1]/sb2
    
    return r2, adj_r2, residuals_df, t1,t2





def range_check(x_baseline, x_reporting):
    status = 0
    reporting_cdd_max = max(x_reporting["CDD"])
    reporting_hdd_max = max(x_reporting["HDD"])
    reporting_cdd_min = min(x_reporting["CDD"])
    reporting_hdd_min = min(x_reporting["HDD"])
    baseline_cdd_max = max(x_baseline["CDD"])
    baseline_hdd_max = max(x_baseline["HDD"])
    baseline_cdd_min = min(x_baseline["CDD"])
    baseline_hdd_min = min(x_baseline["HDD"])
    
    combined_cdd_max = max(reporting_cdd_max, baseline_cdd_max)
    combined_hdd_max = max(reporting_hdd_max, baseline_hdd_max)
    combined_cdd_min = min(reporting_cdd_min,baseline_cdd_min)
    combined_hdd_min = min(reporting_hdd_min, baseline_hdd_min)
    
    effective_cdd_max = combined_cdd_max + 0.05*(combined_cdd_max -combined_cdd_min)
    effective_hdd_max = combined_hdd_max + 0.05*(combined_hdd_max-combined_hdd_min)
    effective_cdd_min = combined_cdd_min - 0.05*(combined_cdd_max - combined_cdd_min)
    effective_hdd_min = combined_hdd_min - 0.05*(combined_hdd_max - combined_hdd_min)
    for i in range(len(x_baseline)):
        if((x_baseline["CDD"][i]<=effective_cdd_max) and (x_baseline["CDD"][i]>=effective_cdd_min) and (x_baseline["HDD"][i]<= effective_hdd_max) and x_baseline["HDD"][i]>=effective_hdd_min):
            status = "OK"
        else:
            status = "Fail"
            break
    return status

  
  
 # Data Frame with measured values THESE TABLES DON'T HAVE CDD & HDD YET
y_baseline_df = pd.read_csv("lilydale_monthly_kwh_baseline.csv")
y_reporting_df = pd.read_csv("lilydale_monthly_kwh_reporting.csv")
baseline_table = pd.read_csv("baseline_table.csv")
reporting_table = pd.read_csv("reporting_table.csv")
    

for i in range(0,30): #CDD
    for k in range (0,30): #HDD

        y_baseline  = y_baseline_df["kwh"]
        x_baseline = baseline_table
        baseline_r2, baseline_adj_r2, baseline_residuals_df, baseline_t1, baseline_t2 = model_linear(x_baseline,y_baseline,i,k)

        y_reporting = y_reporting_df["kwh"]
        x_reporting = reporting_table
        reporting_r2, reporting_adj_r2, reporting_residuals_df, reporting_t1, reporting_t2 = model_linear(x_reporting, y_reporting, i,k)





        if((baseline_r2>=0.75) and (reporting_r2>=0.75) and (i>k) and (baseline_adj_r2>=0.75) and (reporting_adj_r2>=0.75) and(baseline_r2>baseline_adj_r2) and (reporting_r2>reporting_adj_r2)):
            if((reporting_t1>2.26) and (reporting_t2>2.26) and (baseline_t1>2.26) and (baseline_t2>2.26)):

                print()
                print("--------------------------------------------------------------------------------------------")
                print("Baseline R2:",baseline_r2, "  ","Adj R2: ", baseline_adj_r2)
                print("Baseline t - test: t1",baseline_t1, "  t2", baseline_t2)
                print("Operating R2",reporting_r2, "  ","Adj R2: ",reporting_adj_r2)
                print("Reporting t - test: t1",reporting_t1, "  t2", reporting_t2)
                print("CDD:", i,"   ", "HDD", k)
#                 print("Range Check: ",range_check(x_baseline, x_reporting))
                print("--------------------------------------------------------------------------------------------")


