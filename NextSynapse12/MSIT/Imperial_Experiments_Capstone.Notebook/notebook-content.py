# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "67f8983e-c811-4672-9b76-77704bf6075a",
# META       "default_lakehouse_name": "SQLDW",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import matplotlib.pyplot as plt

# Define production capacities and profit per device
capacity_x = 6000
capacity_y = 4000
profit_x = 25.00
profit_y = 30.00

# Calculate the optimal production quantities
defect_rate = 0.05
optimal_production_x = min(capacity_x, 5000)
optimal_production_y = min(capacity_y, 2000)
total_profit = (optimal_production_x * profit_x) + (optimal_production_y * profit_y)

# Calculate profit considering a 5% defect rate
defect_loss_x = optimal_production_x * defect_rate * profit_x
defect_loss_y = optimal_production_y * defect_rate * profit_y
adjusted_profit = total_profit - (defect_loss_x + defect_loss_y)

# Create labels for the bars
labels = ['Total Profit', 'Profit with 5% Defect Rate']

# Create values for the bars
values = [total_profit, adjusted_profit]

# Create a bar plot
plt.bar(labels, values, color=['blue', 'orange'])

# Add titles and labels
plt.title('Profit Comparison')
plt.ylabel('Profit ($)')
plt.ylim(0, max(values) * 1.1)  # Adjust the y-axis range for better visualization

# Display the plot
plt.show()


# MARKDOWN ********************

# ### LIne Grpahs

# CELL ********************

import matplotlib.pyplot as plt

HeartSafe = 5000
SmartAlert = 2000
total_profit = 185000

categories = ["HeartSafe", "SmartAlert", "Total Profit"]


values = [HeartSafe, optimal_B, total_profit]

# Create a line graph
plt.figure(figsize=(10, 6))
plt.plot(categories, values, marker='o', linestyle='-', color='b')
plt.title("Optimal Values")
plt.xlabel("Category")
plt.ylabel("Value")
plt.grid(True)

# Display the graph
plt.show()

# CELL ********************

import matplotlib.pyplot as plt

# Define the scenarios
scenarios = [
    {"Production": "HeartSafe", "Devices": 6000, "Profit": 150000.00},
    {"Production": "SmartAlert", "Devices": 4000, "Profit": 120000.00},
    {"Production": "HeartSafe + SmartAlert", "HeartSafe Devices": 5000, "SmartAlert Devices": 2000, "Profit": 185000.00},
]

# Extract data for plotting
productions = [scenario["Production"] for scenario in scenarios]
profits = [scenario["Profit"] for scenario in scenarios]

# Create a line graph
plt.figure(figsize=(10, 6))

# Plot each scenario separately
for i in range(len(scenarios)):
    plt.plot([productions[i]], [profits[i]], marker='o', linestyle='-', label=f"Scenario {i+1}")

plt.title("Optimal Production and Total Profit")
plt.xlabel("Production Type")
plt.ylabel("Total Profit ($)")
plt.grid(True)

# Add a legend
plt.legend()

# Display the graph
plt.show()


# CELL ********************

import matplotlib.pyplot as plt

optimal_A = 0
optimal_B = 12
total_profit = 92.80

categories = ["Optimal A", "Optimal B", "Total Profit"]


values = [optimal_A, optimal_B, total_profit]

# Create a line graph
plt.figure(figsize=(10, 6))
plt.plot(categories, values, marker='o', linestyle='-', color='b')
plt.title("Optimal Values and Slack")
plt.xlabel("Category")
plt.ylabel("Value")
plt.grid(True)

# Display the graph
plt.show()


# CELL ********************

import matplotlib.pyplot as plt

# Data for the different production options
production_options = ["Heartsafe Only", "SmartAlert Only", "Heartsafe & SmartAlert"]
profits = [150000, 120000, 185000]
max_x_slack = [0, 0, 0]
max_y_slack = [0, 0, 0]
max_hours_slack = [10, 11.4286, 0.714286]

# Create a figure with subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

# Plot 1: Profit comparison
ax1.bar(production_options, profits, color=['blue', 'orange', 'green'])
ax1.set_ylabel('Profit ($)')
ax1.set_title('Profit Comparison for Different Production Options')

# Plot 2: Slack values comparison
ax2.bar(production_options, max_x_slack, label='MaxXDevices.slack', width=0.2, align='center', alpha=0.7)
ax2.bar(production_options, max_y_slack, label='MaxYDevices.slack', width=0.2, align='edge', alpha=0.7)
ax2.bar(production_options, max_hours_slack, label='MaxHours.slack', width=0.2, align='edge', alpha=0.7)
ax2.set_ylabel('Slack')
ax2.set_title('Slack Values Comparison for Different Production Options')
ax2.legend()

# Adjust layout and display plots
plt.tight_layout()
plt.show()

