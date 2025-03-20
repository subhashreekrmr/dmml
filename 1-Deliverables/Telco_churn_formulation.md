# Telco Customer Churn: Problem Formulation

## 1. Business Problem
The telecommunications company aims to reduce customer churn by proactively identifying customers at risk of discontinuing their services. Customer churn directly impacts revenue and profitability, making it critical to predict and mitigate churn through targeted retention strategies.

## 2. Key Business Objectives
- **Churn Prediction**: Accurately predict which customers are likely to churn.
- **Cost Reduction**: Minimize financial spends to advertise and save on loyalty benefits to the customers likely to go away.

## 3. Key Data Sources and Attributes
The dataset 'Telco_customer_churn.xlsx' includes:

| **Category**                | **Attributes**                                                                |
|-----------------------------|-------------------------------------------------------------------------------|
| Customer Demographics       | Gender, Senior Citizen, Partner, Dependents, City, State, Zip Code            |
| Service Usage               | Tenure Months, Phone Service, Multiple Lines, Internet Service, Contract Type |
| Financial Metrics           | Monthly Charges, Total Charges, Payment Method, Paperless Billing             |
| Churn Indicators            | Churn Label (Yes/No), Churn Value (1/0), Churn Reason, Churn Score, CLTV      |
| Technical Features          | Online Security, Online Backup, Device Protection, Tech Support               |
| Geospatial Data             | Latitude, Longitude, Lat Long                                                 |

## 4. Expected Pipeline Outputs
1. **Clean Dataset for EDA**: Preprocessed data for visualization and analysis.
2. **Transformed Features for ML**: Encoded/scaled features (e.g., one-hot encoding, tenure-to-charge ratio).
3. **Deployable Model**: A trained model (e.g., Random Forest) to predict churn.

## 5. Evaluation Metrics
- **Accuracy**: Overall prediction correctness.
- **Precision**: Minimize false positives.
- **Recall**: Maximize true positives.
- **F1-Score**: Balance precision and recall.
- **AUC-ROC**: Performance across classification thresholds.

**Target Metrics**:  
- Precision > 70%, F1-Score > 70%.
- Since Churn is a rare in the customer population precision of the trained models cannot be very high for class churn=Yes/1.

## 6. Deliverables
- Clean dataset (CSV).
- Feature engineering scripts (Python/Jupyter).
- Trained model file (`.pkl` or `.joblib`).
- Evaluation report (PDF/Markdown).
- Deployment guide (PDF).
