# %%
import pandas as pd
import os
import numpy as np

datasets_path = "data/energy"
csv_files: list[str] = []
for root, dirs, files in os.walk(datasets_path):
    for file in files:
        if file.endswith(".csv"):
            csv_files.append(os.path.join(root, file))

data = dict()
for file in csv_files:
    data[file.split('/')[-1].split('.')[0]] = pd.read_csv(file, index_col=0)


# Check that the timestamps are the same for all recordings
key = 'start'
t_diff = np.zeros((len(data), len(data)))
for i, (production_type_A,data_A) in enumerate(data.items()):
    for j, (production_type_B,data_B) in enumerate(data.items()):
        try :
            d = data_A[key] - data_B[key]
            total_diff = np.sum(d)
            average_diff = total_diff/len(d)
            t_diff[i, j] = total_diff
            if total_diff != 0:
                print(f"{production_type_A} vs {production_type_B} average diff : {average_diff}")
        except KeyError as e:
            print(production_type_A)

print(t_diff)

# %%
# Fuse data into 1 dataframe
fused_dataframe = pd.DataFrame({
    **{'Time': data['SOLAR']['start']},
    **{k: v['values'] for k, v in data.items()}
})

# %%
import matplotlib
import matplotlib.pyplot as plt

matplotlib.use('tkagg')  # Use GTK3 backend for plotting


plt.figure(figsize=(20, 10))
for column in fused_dataframe.columns:
    if column == 'Time':
        continue
    plt.plot(fused_dataframe['Time'], fused_dataframe[column], label=column)
plt.xlabel("Time")
plt.ylabel("Energy Production")
plt.legend()
plt.grid()
plt.show()

for column in fused_dataframe.columns:
    if column in ['Time', 'TOTAL']:
        continue

    contribution = np.sum(
        fused_dataframe[column]) / np.sum(fused_dataframe['TOTAL'])
    print(f"{column.capitalize()} energy accounts for {contribution*100:.1f}% of total energy production.")


