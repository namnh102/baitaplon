from sklearn.metrics import silhouette_score
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

# Tính toán chỉ số silhouette cho các giá trị khác nhau của K
silhouette_scores = []

data = pd.read_csv('results.csv')
# Chọn các cột dữ liệu dạng số
numerical_data = data.select_dtypes(include=['float64', 'int64']).dropna(axis=1)

# Chuẩn hoá dữ liệu
scaler = StandardScaler()
data_scaled = scaler.fit_transform(numerical_data)

# Duyệt qua các giá trị K từ 2 đến 10 (bắt đầu từ 2 vì silhouette không xác định cho K=1)
for k in range(2, 11):
    kmeans = KMeans(n_clusters=k, random_state=0)
    cluster_labels = kmeans.fit_predict(data_scaled) # Gán nhãn cụm cho dữ liệu
    score = silhouette_score(data_scaled, cluster_labels) # Tính chỉ số silhouette
    silhouette_scores.append(score)

# Vẽ biểu đồ Silhouette Score cho từng giá trị K
plt.figure(figsize=(10, 6))
plt.plot(range(2, 11), silhouette_scores, marker='o', linestyle='-')
plt.xlabel('Số lượng cụm (K)')
plt.ylabel('Silhouette Score')
plt.title('Chỉ số Silhouette cho các giá trị K khác nhau')
plt.show()