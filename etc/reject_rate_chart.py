def generate_pie_chart(labels, ratios, colors, title, figsize=(10, 8), startangle=90, pctdistance=1):
    """
    Generate a pie chart with the given parameters.

    Args:
        labels (list): List of labels for the pie chart sections.
        ratios (list): Corresponding percentages for each section.
        colors (list): List of colors for each section.
        title (str): Title of the pie chart.
        figsize (tuple): Size of the figure.
        startangle (int): Start angle for the pie chart.
    """
    from matplotlib import pyplot as plt

    
    plt.figure(figsize=figsize)
    wedges, texts, autotexts = plt.pie(
        ratios,
        labels=labels,
        autopct='%1.1f%%',
        colors=colors,
        startangle=startangle,
        textprops={'fontsize': 14, 'fontweight': 'bold'},  # Updated font size and bold
        wedgeprops={'edgecolor': 'white', 'linewidth': 0.5},
        pctdistance=pctdistance
    )

    # Style the percentage labels for better visibility
    for autotext in autotexts:
        autotext.set_color("black")
        autotext.set_fontsize(13)

    # Add title and layout
    plt.title(title, fontsize=14)
    plt.tight_layout()
    plt.show()


# Data for the pie chart
labels = [
    "Reject the\nwhole tote",
    "Out of scope\nitem stuck",
    "Manually reject to QA",
    "Mismatch with WMS or\nManually item rejected before tote income",
    "Scan fail - Wrinkled barcode",
    "Bagger error\n- Operator issue",
    "Cancelled items",
    "Coupang item issue",
    "Scan fail - Small barcode",
    "Fail to scan the label\n- Operator issue\n",
    "Double pick",
    "Scan fail - OSARO issue",
    "Item dropped to QA",
    "Item dropped on floor\n\n"
]
ratios = [29.2, 11.0, 10.4, 9.1, 9.1, 5.8, 5.2, 3.3, 2.0, 2.0, 6.5, 2.6, 2.6, 1.3]
colors = [
    "#0071CE", "#0071CE", "#0071CE", "#0071CE", "#0071CE", "#0071CE", "#0071CE", "#0071CE", "#0071CE", "#0071CE",
    "#FF6347", "#FF6347", "#FF6347", "#FF6347"
]
# #FA8072, #FF6347

# Generate the chart
generate_pie_chart(
    labels=labels,
    ratios=ratios,
    colors=colors,
    title="Reject Reasons with ratio",
    pctdistance=0.75
)
