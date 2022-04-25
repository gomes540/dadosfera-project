import glob
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from settings import DataPath, DataFilter, DataSchema

# Get all filenames from data folders
trips_filenames = glob.glob(DataPath.TRIPS_PATH.value + "/*.json")
payment_filenames = glob.glob(DataPath.PAYMENT_PATH.value + "/*.csv")
vendor_filenames = glob.glob(DataPath.VENDOR_PATH.value + "/*.csv")

appended_trip_jsons = []
for filename in trips_filenames:
    appended_trip_jsons.append(pd.read_json(filename, lines=True, dtype=DataSchema.TRIPS_SCHEMA.value))

trips_dataframe = pd.concat(appended_trip_jsons)

# Lower all payment_type values
trips_dataframe["payment_type"] = trips_dataframe["payment_type"].str.lower()

# Questão 01: Qual a distância média percorrida por viagens com no máximo 2 passageiros?

avg_distance_for_less_then_2_passenger = trips_dataframe[
    trips_dataframe["passenger_count"] <= DataFilter.PASSENGER_FILTER.value]["trip_distance"].mean()

print(
    f"Distância média percorrida por viagens com no máximo 2 passageiros: {round(avg_distance_for_less_then_2_passenger, 2)} metros \n"
)

# Questão 02: Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado

top_3_vendors = (
    trips_dataframe.groupby(by="vendor_id", as_index=False)["total_amount"]
    .sum()
    .sort_values(by="total_amount", ascending=False, ignore_index=True)[:3]
)

top_3_vendors['total_amount'] = top_3_vendors['total_amount'] / 1000000

print("Top 3 vendors que mais arrecadaram: \n")
print(top_3_vendors.head())

q02 = sns.barplot(data=top_3_vendors, x="vendor_id", y="total_amount")
q02.set(xlabel='Vendors', ylabel='Total Million Amount ($)')
q02.set_title('Top 3 Vendors')
plt.savefig("answer/q02/top_3_vendors.png")

# Questão 03: Faça um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro

trips_months_dataframe = trips_dataframe.copy()
trips_months_dataframe["year_month"] = trips_months_dataframe["pickup_datetime"].dt.strftime("%Y/%m")
cash_trips_months_dataframe = trips_months_dataframe[trips_months_dataframe["payment_type"] == DataFilter.PAYMENT_FILTER.value]

grouped_trips_months_dataframe = trips_months_dataframe.groupby("year_month", as_index=False)["total_amount"].sum()

grouped_trips_months_dataframe["total_amount"] = (grouped_trips_months_dataframe["total_amount"] / 1000000)

# Create plot for question 03
sns.set(rc={"figure.figsize": (40.7, 20.7)})
sns.set(font_scale=2)
q03 = sns.barplot(data=grouped_trips_months_dataframe, x="year_month", y="total_amount")
q03.set(xlabel='Date', ylabel='Total Million Amount ($)')
q03.set_title('Monthly total amount paid in cash')
plt.xticks(rotation=45)
plt.savefig("answer/q03/monthly_total_amount_paid_in_cash.png")


# Questão 04: Faça um gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos últimos 3 meses de 2012.

last_3_month_of_2012_trip_df = trips_dataframe[
    (trips_dataframe["pickup_datetime"].dt.strftime("%Y-%m-%d") >= "2012-08-01")
    & (trips_dataframe["pickup_datetime"].dt.strftime("%Y-%m-%d") <= "2012-10-31")
]

last_3_month_of_2012_trip_df['pickup_datetime'] = last_3_month_of_2012_trip_df['pickup_datetime'].dt.strftime("%Y-%m-%d")

last_3_month_of_2012_trip_df.sort_values(by='pickup_datetime', inplace=True)

# Create plot for question 04
plt.clf()
sns.set(font_scale=2)
q04 = sns.lineplot(data=last_3_month_of_2012_trip_df, x="pickup_datetime", y="tip_amount", ci=None, markers=True)
q04.set(xlabel='Pickup Date', ylabel='Tip Amount ($)')
q04.set_title('Tips per day in the last 3 months of 2012')
plt.xticks(rotation=80)
plt.savefig("answer/q04/tips_per_day_in_the_last_3_months_of_2012.png")
