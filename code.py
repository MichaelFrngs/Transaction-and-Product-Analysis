import pandas as pd
import matplotlib as plt
import os
import datetime as dt

def get_df_name(df):
    name =[x for x in globals() if globals()[x] is df][0]
    return name

def CurrentMonth(monthData):
  Data = monthData
  Name = get_df_name(monthData)
  return  Data, Name

###initialize variable
Summary = pd.DataFrame(columns = ['Group','# Trans','Sales','Costs','InitialMargin',
                       'InitialMarginPCT','Shipping_Costs','Margin_after_shipping',
                       'MarginPCT_after_shipping','Total Weight','Avg # Trans','Avg Sales','Avg Costs','Avg InitialMargin',
                       'Avg Shipping_Costs','Avg Margin_after_shipping','Avg Weight'])


#Name data source directory
SkuSales_directory = "Z:\P&Ls - Web\Sku Sales Queries"

os.chdir("Z:/Janette/P&Ls - Web")

divisions = ["CONSUMABLES         ", "HARDLINES           ","SPECIALTY           ", "Aggregate"]
###################select 0 through 3 from above list ########################################
#selectDivision = divisions[2]  #Note: Aggregate computes all three.

#load data. MUST FOLLOW THIS NAMING CONVENTION
os.chdir(f"{SkuSales_directory}")
OctoberData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web October 2019.xlsx"))
SeptemberData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web September 2019.xlsx"))
AugustData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web August 2019.xlsx"))
JulyData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web July 2019.xlsx"))
JuneData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web June 2019.xlsx",'June 2019 Sales Data'))
MayData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web May 2019.xlsx",'Sheet1'))


OMS_Data = pd.read_excel("//data/accounting/P&Ls - Web/OMS Sales Journals/OMS_Data.xlsx")


#Place the long fedex file here
MergeAttemptFedex = pd.read_excel("//petdata/accounting/P&Ls - Web/2019 Fedex Shipping Data/Fedex (jan 2019 to Nov 18 2019).xlsx")

#Place the OMS data here
MergeAttemptOMS = pd.read_excel("//petdata/accounting/P&Ls - Web/OMS Sales Journals/OMS_Data.xlsx")

#We're having issues with "Ticket #" column having upper & lower case variations across datasets
MergeAttemptOMS.columns = ['TICKET #', 'Customer', 'Reference Notes Line 1', 'Merchandise', 'Freight',
       'Addl freight', 'Tax', 'Handling', 'Addl charge', 'Total cash',
       'Total C over C', 'Shipment_Date', 'Unique_Customer', 'Unique_Order']
Merged_Data = MergeAttemptFedex.merge(MergeAttemptOMS, how = "outer", on = "Reference Notes Line 1")

#iterate through this list
DataList = [MayData,JuneData,JulyData, AugustData, SeptemberData, OctoberData]

Accumulated_sku_sales = pd.DataFrame()
for Month in DataList:
  sku_sales_query_pivot = Month.pivot_table(index = "TICKET #", values = ["SALES RETAIL","SALES UNITS","EXT COST","GM"]).reset_index()
  Accumulated_sku_sales = Accumulated_sku_sales.append(sku_sales_query_pivot)
Merged_Data = Merged_Data.merge(Accumulated_sku_sales, how = "outer", on = "TICKET #")

    

#To lowercase the ticket number column
Merged_Data.columns = ['Payer Account', 'Invoice Month (yyyymm)', 'OPCO', 'Service Type',
       'Shipment Date', 'Shipment Delivery Date', 'Shipment Tracking Number',
       'Shipper City', 'Shipment Freight Charge Amount USD',
       'Shipment Miscellaneous Charge USD', 'Shipment Duty and Tax Charge USD',
       'Shipment Discount Amount USD', 'Net Charge Amount USD',
       'Pieces in Shipment', 'Shipment Rated Weight(Pounds)',
       'Original weight(Pounds)', 'Proof of Delivery Recipient',
       'Recipient Name', 'Recipient Company Name', 'Recipient Address',
       'Recipient City', 'Recipient State', 'Recipient Postal Code',
       'Reference Notes Line 1', 'Reference Notes Line 2', 'Department Number',
       'PO Number', 'Pricing Zone', 'Invoice date (mm/dd/yyyy)',
       'Invoice number', 'Shipment Delivery Time', 'Ticket #', 'Customer',
       'Merchandise', 'Freight', 'Addl freight', 'Tax', 'Handling',
       'Addl charge', 'Total cash', 'Total C over C', 'Shipment_Date',
       'Unique_Customer', 'Unique_Order', 'EXT COST', 'GM', 'SALES RETAIL',
       'SALES UNITS']
                                
Freight_and_Margin_Data = Merged_Data

for division in divisions:
  DIVISION = division      
    
  
  for Month in DataList:
    #######Select a month to process, or two months, or more######
    #AggregateData = pd.concat([MayData, JuneData]) #Brenda said she wants June and May together for now
    CurrentMonthData = Month
    
    #Returns a string of the current month
    MonthNameString = CurrentMonth(CurrentMonthData)[1].replace('Data','')
    
    #Variable directory for each month
    Export_Directory = f"Z:/P&Ls - Web/Product and Transaction Analysis/{MonthNameString}"
    
    #Import the current month's data
    AggregateData = pd.concat([CurrentMonth(CurrentMonthData)[0]])
    
    
    #Test out some dates
    #AggregateData[(AggregateData["DATE"] == "5/6/2019") & (AggregateData["TRANS"] == "N")].sum()
    #AggregateData[(AggregateData["DATE"] == "5/15/2019") & (AggregateData["TRANS"] == "N")].sum()
    #AggregateData[(AggregateData["DATE"] == "5/23/2019") & (AggregateData["TRANS"] == "N")].sum()
    
    
    #Filters out ghost transactions
    if DIVISION == "Aggregate":
      FilteredAggregateData = AggregateData[(AggregateData["TRANS"] == "N")]
    else:
      FilteredAggregateData = AggregateData[(AggregateData["TRANS"] == "N") & (AggregateData["DIVISION"] == f"{DIVISION}")]  
    
    FilteredAggregateData.sum()
    
    vendorsList = set(FilteredAggregateData["VENDOR NAME"])
    print("Number of vendors = ", len(vendorsList), "\n", vendorsList)
    
    
    
    
    #Which statisic above will we use in the formulas through the program?
    EstimateShippingCost = ModeShippingCost
    
    #ITEM ANALYSIS
    ItemDataFrame = pd.DataFrame(columns = ["Item", "NumSold", "EXT SALES", "EXT COSTS", "Profit","Profit_Margin", "Est_Margin_after_shipping"])
    Reconciliation = []
    ItemsPurchasedList = set(FilteredAggregateData["SKU DESCRIPTION"])
    i=1
    for Item in ItemsPurchasedList:
        ItemRows = FilteredAggregateData[FilteredAggregateData["SKU DESCRIPTION"] == Item]
        NumSold = len(ItemRows)
        ExtSales  =(ItemRows["SALES RETAIL"].sum()) #Switches between SALES and SALES RETAIL depending on data source. Consider static names
        ExtCosts = (ItemRows["EXT COST"].sum())
        Profit = ItemRows["SALES RETAIL"].sum() - ItemRows["EXT COST"].sum()
        ProfitMargin = (ExtSales - ExtCosts)/ExtSales
        Est_Profit_after_shipping = Profit - EstimateShippingCost
        Est_Margin_after_shipping = (ExtSales - ExtCosts - EstimateShippingCost)/ExtSales
    
        #New line of data to be appended to dataframe iteratively
        NewLine = {"Item" : Item, "NumSold" : NumSold, "EXT SALES" : ExtSales,"EXT COSTS" : ExtCosts, "Profit" : Profit, "Profit_Margin":ProfitMargin,
                   "Est_Profit_after_shipping":Est_Profit_after_shipping, "Est_Margin_after_shipping":Est_Margin_after_shipping}
        
        ItemDataFrame = ItemDataFrame.append(NewLine, ignore_index = True)
    
    os.chdir(f"{Export_Directory}")
    ItemDataFrame.to_excel(f"{CurrentMonth(CurrentMonthData)[1].replace('Data','')} {DIVISION} Product Analysis.xlsx")
        
    
    
    
    
    #    print(f"Number of {Item} sold = ", NumSold,
    #          "   | Sales = $", ExtSales,
    #          "   | Cost = $", ExtCosts,
    #          "   | Profit = $", Profit)
    Reconciliation.append(ItemRows["SALES RETAIL"].sum()) 
    print("Reconciliation = ", pd.DataFrame(Reconciliation).sum())
    
       
    
    Transactions = set(FilteredAggregateData["TICKET #"])
    print("# of transactions = ",len(Transactions))   
    
          
          
    
          
            #ADD SHIPPING STUFF HERE
    #TRANSACTION ANALYSIS
    TransactionsDataFrame = pd.DataFrame(columns = ["TICKET_NUM", "NumOfItemsInTransaction", "TRANSACTION_EXT SALES", "TRANSACTION_EXT COSTS",
                                                    "TRANSACTION_Profit", "InitialMargin_%",
                                                    "Shipping_Costs","Margin_after_shipping","MarginPCT_after_shipping",
                                                    "item_1_sold", "item_2_sold", "item_3_sold", "All_Items_Sold"])
    NumberOfItemsPurchasedList = []
    for Trnsction in Transactions:
        #Return item row data for each transaction
        ItemRows = FilteredAggregateData[FilteredAggregateData["TICKET #"] == Trnsction]
        RelevantOMS_data_Rows = OMS_Data[OMS_Data["Ticket #"] == Trnsction]
        Relevant_Shipping_Data = Freight_and_Margin_Data[Freight_and_Margin_Data["Ticket #"] == Trnsction]
        #print(len(ItemRows))
        NumOfItemsInTransaction = len(ItemRows)
        ExtSales  =(ItemRows["SALES RETAIL"].sum())
        ExtCosts = (ItemRows["EXT COST"].sum())
        Profit = ItemRows["SALES RETAIL"].sum() - ItemRows["EXT COST"].sum()
        #Shipping costs
        if len(RelevantOMS_data_Rows) == 0:
          Shipping_Costs = 0
        else:
          Shipping_Costs = RelevantOMS_data_Rows["Freight"] + RelevantOMS_data_Rows["Addl freight"]
          
        if len(Relevant_Shipping_Data) == 0:
          Weight = 0
        else:
          Weight = Relevant_Shipping_Data["Shipment Rated Weight(Pounds)"].sum()
          
        
          
        InitialMarginPct = (ExtSales - ExtCosts) / ExtSales
        #Est_Margin_after_shipping = (ExtSales - ExtCosts - EstimateShippingCost)/ExtSales
        #Est_Profit_after_shipping = Profit - EstimateShippingCost
        Margin_after_shipping = Profit - Shipping_Costs
        MarginPCT_after_shipping = (ExtSales - ExtCosts - Shipping_Costs)/ExtSales
        
        if NumOfItemsInTransaction == 1:
          #print(type(ItemRows))
          item_1_sold = ItemRows["SKU DESCRIPTION"]
          item_2_sold = None
          item_3_sold = None
          All_Items_Sold = ItemRows["SKU DESCRIPTION"]
        elif NumOfItemsInTransaction == 2:
          item_1_sold = ItemRows["SKU DESCRIPTION"].iloc[0]
          item_2_sold = ItemRows["SKU DESCRIPTION"].iloc[1]
          item_3_sold = None
          All_Items_Sold = ItemRows["SKU DESCRIPTION"]
        elif NumOfItemsInTransaction == 3:
          item_1_sold = ItemRows["SKU DESCRIPTION"].iloc[0]
          item_2_sold = ItemRows["SKU DESCRIPTION"].iloc[1]
          item_3_sold = ItemRows["SKU DESCRIPTION"].iloc[2]
          All_Items_Sold = ItemRows["SKU DESCRIPTION"]
        elif NumOfItemsInTransaction > 3:
          All_Items_Sold = ItemRows["SKU DESCRIPTION"]
        #Can be extended if needed
        
        #for debugging purposes
        NumberOfItemsPurchasedList.append(NumOfItemsInTransaction)
        
        #iteratively adds new rows to the empty dataframe
        NewLine = {"TICKET_NUM" : Trnsction, "NumOfItemsInTransaction" : NumOfItemsInTransaction, "TRANSACTION_EXT SALES" : ExtSales,"TRANSACTION_EXT COSTS" : ExtCosts,
                   "TRANSACTION_Profit" : Profit, "InitialMargin_%":InitialMarginPct,
                   "All_Items_Sold":All_Items_Sold, 
                   #"Est_Profit_after_shipping":Est_Profit_after_shipping, "Est_Margin_after_shipping":Est_Margin_after_shipping, 
                   "Shipping_Costs":float(Shipping_Costs),
                   "Margin_after_shipping":float(Margin_after_shipping),"MarginPCT_after_shipping":float(MarginPCT_after_shipping),
                   "Weight": float(Weight),
                   "item_1_sold" : item_1_sold , "item_2_sold" : item_2_sold, "item_3_sold" : item_3_sold}
        
        TransactionsDataFrame = TransactionsDataFrame.append(NewLine, ignore_index = True)
        
    os.chdir(f"{Export_Directory}")
    TransactionsDataFrame.to_excel(f"{CurrentMonth(CurrentMonthData)[1].replace('Data','')} {DIVISION} Transaction Analysis.xlsx")
    
    
    
    
    
    
    def Average(lst): 
        return sum(lst) / len(lst) 
    print("Average Number of Items purchased = ", Average(NumberOfItemsPurchasedList))
    
    
    
    TransactionsOver49Dollars =  TransactionsDataFrame[TransactionsDataFrame["TRANSACTION_EXT SALES"] > 49]
    TransactionsUnder49Dollars = TransactionsDataFrame[TransactionsDataFrame["TRANSACTION_EXT SALES"] < 49]                           
    TransactionsOver49Dollars.to_excel(f"{MonthNameString} {DIVISION}TransactionsOver49.xlsx")
    TransactionsUnder49Dollars.to_excel(f"{MonthNameString} {DIVISION}TransactionsUnder49.xlsx")
    
    
    
    
    
    
    
    #Summary compilation section
    
    # 					 Est Profit After Shipping 	Est Margin after shipping	Ship Rev	Ship Cost	Net Shipping	# items in shipment	Avg Weight
    SummaryTemp = pd.DataFrame(
                        {'Group':                    [f"{MonthNameString} {DIVISION.replace(' ','')}",">$ 49", "<$ 49"],
                         '# Trans':                  [len(TransactionsDataFrame["TICKET_NUM"]),len(TransactionsOver49Dollars),len(TransactionsUnder49Dollars)],
                         'Sales':                    [TransactionsDataFrame["TRANSACTION_EXT SALES"].sum(),TransactionsOver49Dollars["TRANSACTION_EXT SALES"].sum(),TransactionsUnder49Dollars["TRANSACTION_EXT SALES"].sum() ],
                         'Costs':                    [TransactionsDataFrame['TRANSACTION_EXT COSTS'].sum(),TransactionsOver49Dollars['TRANSACTION_EXT COSTS'].sum(),TransactionsUnder49Dollars['TRANSACTION_EXT COSTS'].sum() ],
                         'InitialMargin':            [TransactionsDataFrame["TRANSACTION_Profit"].sum(),   TransactionsOver49Dollars["TRANSACTION_Profit"].sum(),   TransactionsUnder49Dollars["TRANSACTION_Profit"].sum()  ],
                         
                         'InitialMarginPCT':         [(TransactionsDataFrame["TRANSACTION_EXT SALES"].sum()-TransactionsDataFrame['TRANSACTION_EXT COSTS'].sum())/TransactionsDataFrame["TRANSACTION_EXT SALES"].sum(),
                                                      (TransactionsOver49Dollars["TRANSACTION_EXT SALES"].sum()-TransactionsOver49Dollars['TRANSACTION_EXT COSTS'].sum())/TransactionsOver49Dollars["TRANSACTION_EXT SALES"].sum(),
                                                      (TransactionsUnder49Dollars["TRANSACTION_EXT SALES"].sum()-TransactionsUnder49Dollars['TRANSACTION_EXT COSTS'].sum())/TransactionsUnder49Dollars["TRANSACTION_EXT SALES"].sum()],
                         
                         'Shipping_Costs':           [TransactionsDataFrame["Shipping_Costs"].sum(),TransactionsOver49Dollars["Shipping_Costs"].sum(),TransactionsUnder49Dollars["Shipping_Costs"].sum() ],
                         'Margin_after_shipping':    [TransactionsDataFrame['Margin_after_shipping'].sum(),TransactionsOver49Dollars['Margin_after_shipping'].sum(),TransactionsUnder49Dollars['Margin_after_shipping'].sum() ],
                         
                         'MarginPCT_after_shipping': [(TransactionsDataFrame["TRANSACTION_Profit"].sum()-TransactionsDataFrame['Shipping_Costs'].sum())/TransactionsDataFrame["TRANSACTION_EXT SALES"].sum(),
                                                      (TransactionsOver49Dollars["TRANSACTION_Profit"].sum()-TransactionsOver49Dollars['Shipping_Costs'].sum())/TransactionsOver49Dollars["TRANSACTION_EXT SALES"].sum(),
                                                      (TransactionsUnder49Dollars["TRANSACTION_Profit"].sum()-TransactionsUnder49Dollars['Shipping_Costs'].sum())/TransactionsUnder49Dollars["TRANSACTION_EXT SALES"].sum()],
                         
                         'Total Weight':             [TransactionsDataFrame["Weight"].sum(),TransactionsOver49Dollars["Weight"].sum(),TransactionsUnder49Dollars["Weight"].sum() ],
                    
                         #AVERAGES SECTION
                         'Avg # Trans':              [len(TransactionsDataFrame["TICKET_NUM"]),len(TransactionsOver49Dollars),len(TransactionsUnder49Dollars)],
                         'Avg Sales':                [TransactionsDataFrame["TRANSACTION_EXT SALES"].mean(),TransactionsOver49Dollars["TRANSACTION_EXT SALES"].mean(),TransactionsUnder49Dollars["TRANSACTION_EXT SALES"].mean() ],
                         'Avg Costs':                [TransactionsDataFrame['TRANSACTION_EXT COSTS'].mean(),TransactionsOver49Dollars['TRANSACTION_EXT COSTS'].mean(),TransactionsUnder49Dollars['TRANSACTION_EXT COSTS'].mean() ],
                         'Avg InitialMargin':        [TransactionsDataFrame["TRANSACTION_Profit"].mean(),   TransactionsOver49Dollars["TRANSACTION_Profit"].mean(),   TransactionsUnder49Dollars["TRANSACTION_Profit"].mean()  ],
                        
                         
                         'Avg Shipping_Costs':           [TransactionsDataFrame["Shipping_Costs"].mean(),TransactionsOver49Dollars["Shipping_Costs"].mean(),TransactionsUnder49Dollars["Shipping_Costs"].mean() ],
                         'Avg Margin_after_shipping':    [TransactionsDataFrame['Margin_after_shipping'].mean(),TransactionsOver49Dollars['Margin_after_shipping'].mean(),TransactionsUnder49Dollars['Margin_after_shipping'].mean() ],
                         'Avg Weight':                   [TransactionsDataFrame["Weight"].mean(),TransactionsOver49Dollars["Weight"].mean(),TransactionsUnder49Dollars["Weight"].mean() ]


                         
                         
                       
                         })
              
    Summary = Summary.append(SummaryTemp)

import datetime as dt    
os.chdir("Z:/Janette/P&Ls - Web")
Summary.to_excel(f"Web P&L Transaction & Product Analysis Summary {str(dt.datetime.now())[:10]}.xlsx")
                                                     



#VISUALIZE
#VISUALIZEs
#VISUALIZE
from mpl_toolkits import mplot3d
%matplotlib inline
import numpy as np
import matplotlib.pyplot as plt

#This piece of code must be run together with the other chunks below for this to work.
fig = plt.figure()
ax = plt.axes(projection='3d')

z_name = "GM"
x_name = "Merchandise"
y_name = "Net Charge Amount USD"

ax.set_xlabel(f'{x_name}', fontsize=10, rotation=150)
ax.set_ylabel(f'{y_name}')
ax.set_zlabel(f"{z_name}", fontsize=10, rotation=90)

# Data for a three-dimensional line (helix)
#zline = np.linspace(0, 15, 1000) #Divides 15 to zero in 1000 equal parts.
#xline = np.sin(zline)
#yline = np.cos(zline)
#ax.plot3D(xline, yline, zline, 'gray')

# Data for three-dimensional scattered points around the drawn helix line
zdata = Merged_Data[f"{z_name}"]
xdata = Merged_Data[f"{x_name}"] 
ydata = Merged_Data[f"{y_name}"] 
ax.scatter3D(xdata, ydata, zdata, c=Merged_Data["Shipment Rated Weight(Pounds)"], cmap = "Dark2_r", alpha = 0.15);      
