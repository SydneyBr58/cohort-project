import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

fig, ax = plt.subplots(figsize=(20,8))
df = pd.read_csv('table_month.csv')
df['date'] = pd.to_datetime(df['date'])
for i in range(1,10):
    x = df['date'][df['nbr']==i].to_numpy()
    y= df['avg_usage'][df['nbr']==i].to_numpy()
    ax.plot(x,y, label='Group '+str(i))

ax.set_xlabel('date')
ax.set_ylabel('Average days of use per month')
ax.set_title('Average usage by cohort as a function of time')

# Major ticks every 3 months.
fmt_half_year = mdates.MonthLocator(interval=3)
ax.xaxis.set_major_locator(fmt_half_year)

# Minor ticks every month.
fmt_month = mdates.MonthLocator()
ax.xaxis.set_minor_locator(fmt_month)

# Text in the x axis will be displayed in 'YYYY-mm' format.
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

ax.grid()
ax.legend(title='Cohorts', bbox_to_anchor=(1, 1), loc='upper left')