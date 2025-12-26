from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *

import pandas as pd
import matplotlib.pyplot as plt

session = get_active_session()


customers = session.table("ANALYTICS.CUSTOMER_SILVER") 
orders = session.table("ANALYTICS.ORDERS_SILVER")
print("Load completed")

customers.cache_result()
orders.cache_result()

# 5.1 Customer Segmentation w RFM (Recency, Frequency, Monetary)
print("Starting RFM Segmentation...")
# Calculate RFM metrics
rfm_df = (
    customers
    .join(orders, customers["C_CUSTKEY"] == orders["O_CUSTKEY"], "left")
    .group_by("C_CUSTKEY", "C_NAME")
    .agg([
        max("O_ORDERDATE").alias("LAST_ORDER_DATE"),
        count("O_ORDERKEY").alias("FREQUENCY"),
        sum("O_TOTALPRICE").alias("MONETARY")
    ])
    .with_column("RECENCY_DAYS", datediff("day", col("LAST_ORDER_DATE"), current_date()))
)

# Save to table
rfm_df.write.mode("overwrite").save_as_table("ANALYTICS.CUSTOMER_RFM_SCORES")

print(f"✅ RFM Segmentation completed!")
print(f"   Total customers processed: {rfm_df.count()}")

# Show sample
rfm_df.cache_result()
rfm_df.filter(col("FREQUENCY") > 0).show(10)

# 5.2 Sales Trend Analysis

monthly_sales = (orders
    .with_column("MONTH", date_trunc("month", col("O_ORDERDATE")))
    .group_by("MONTH")
    .agg([
        count("O_ORDERKEY").alias("ORDER_COUNT"),
        sum("O_TOTALPRICE").alias("TOTAL_REVENUE"),
        avg("O_TOTALPRICE").alias("AVG_ORDER_VALUE")
    ])
    .sort("MONTH")
)

monthly_sales.cache_result()


# Convert to pandas for visualization
df_pandas = monthly_sales.to_pandas()

df_pandas['MONTH'] = pd.to_datetime(df_pandas['MONTH'])

fig, ax1 = plt.subplots(figsize=(12, 6))

# --- Axis main Y (Column Chart for Doanh Thu) ---
color_revenue = '#FFA240'
ax1.set_xlabel('Tháng')
ax1.set_ylabel('Tổng Doanh Thu (TOTAL_REVENUE)', color=color_revenue)

# Draw (Bar Chart)
ax1.bar(
    df_pandas['MONTH'], 
    df_pandas['TOTAL_REVENUE'], 
    color=color_revenue, 
    label='Tổng Doanh Thu', 
    width=20,
    zorder=1
)
ax1.tick_params(axis='y', labelcolor=color_revenue)
ax1.grid(True, axis='y')
ax1.patch.set_visible(False)

# 2. Draw(ax2) for Line Chart
ax2 = ax1.twinx()  
color_avg = '#0F2854'
ax2.set_ylabel('Giá Trị Đơn Hàng TB (AVG_ORDER_VALUE)', color=color_avg)  

# Draw (Line Chart)
ax2.plot(
    df_pandas['MONTH'], 
    df_pandas['AVG_ORDER_VALUE'], 
    color=color_avg, 
    linestyle='-', 
    marker='o',
    label='Giá Trị Đơn Hàng TB',
    zorder=3
)
ax2.tick_params(axis='y', labelcolor=color_avg)

plt.title('Doanh Thu Hàng Tháng (Column) và Giá Trị Đơn Hàng TB (Line)')
plt.gcf().autofmt_xdate()

plt.show()
# plt.savefig('revenue_vs_aov_fixed.png')

