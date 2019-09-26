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
                       'MarginPCT_after_shipping','Avg # Trans','Avg Sales','Avg Costs','Avg InitialMargin','Avg Shipping_Costs','Avg Margin_after_shipping'])

 

#Name data source directory
SkuSales_directory = "Z:\Janette\P&Ls - Web\Sku Sales Queries"

os.chdir("Z:/P&Ls")

divisions = ["CONSUMABLES         ", "HARDLINES           ","SPECIALTY           ", "Aggregate"]
###################select 0 through 3 from above list ########################################
#selectDivision = divisions[2]  #Note: Aggregate computes all three.

for division in divisions:
  DIVISION = division      #DIVISION = selectDivision           
  
  #Load data. MUST FOLLOW THIS NAMING CONVENTION
  os.chdir(f"{SkuSales_directory}")
  SeptemberData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web September 2019.xlsx"))
  AugustData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web August 2019.xlsx"))
  JulyData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web July 2019.xlsx"))
  JuneData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web June 2019.xlsx",'June 2019 Sales Data'))
  MayData = pd.DataFrame(pd.read_excel("Sku Sales Query for Web May 2019.xlsx",'Sheet1'))
  Freight_and_Margin_Data = pd.read_excel("Z:/P&Ls - Web/Merged Datasets/Sku Sales, Fedex, OMS.xlsx", "Shipment Detail (Fedex)")
  OMS_Data = pd.read_excel("Z:/P&Ls - Web/Merged Datasets/Sku Sales, Fedex, OMS.xlsx", "OMS Data 5-15-19 to 09-06-19")
  
  #List of data to export
  DataList = [JulyData, AugustData, SeptemberData]
  
  for Month in DataList:
    #######Select a month to process, or two months, or more######
    #AggregateData = pd.concat([MayData, JuneData]) 
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
    
    
    
    
    MeanShippingCost = 13.33
    MedianShippingCost = 9.91
    ModeShippingCost = 9.42
  
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
                   "item_1_sold" : item_1_sold , "item_2_sold" : item_2_sold, "item_3_sold" : item_3_sold}
        
        TransactionsDataFrame = TransactionsDataFrame.append(NewLine, ignore_index = True)
        
    os.chdir(f"{Export_Directory}")
    TransactionsDataFrame.to_excel(f"{CurrentMonth(CurrentMonthData)[1].replace('Data','')} {DIVISION} Transaction Analysis.xlsx")
    
    
    
    
    
    
    
    #JuneshippingData = pd.DataFrame(pd.read_excel("
    
    
    
    def Average(lst): 
        return sum(lst) / len(lst) 
    print("Average Number of Items purchased = ", Average(NumberOfItemsPurchasedList))
    
    
    
    TransactionsOver49Dollars =  TransactionsDataFrame[TransactionsDataFrame["TRANSACTION_EXT SALES"] > 49]
    TransactionsUnder49Dollars = TransactionsDataFrame[TransactionsDataFrame["TRANSACTION_EXT SALES"] < 49]                           
    TransactionsOver49Dollars.to_excel(f"{MonthNameString} {DIVISION}TransactionsOver49.xlsx")
    TransactionsUnder49Dollars.to_excel(f"{MonthNameString} {DIVISION}TransactionsUnder49.xlsx")
    
    
    
    
    
    
    
    #Summary compilation section
    
    # 					 Est Profit After Shipping 	Est Margin after shipping	Ship Rev	Ship Cost	Net Shipping	# items in shipment	Avg Weight
    SummaryTemp = pd.DataFrame({'Group':                 [f"{MonthNameString} {DIVISION.replace(' ','')}",">$ 49", "<$ 49"],
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
     
                    
                         #AVERAGES SECTION
                         'Avg # Trans':                  [len(TransactionsDataFrame["TICKET_NUM"]),len(TransactionsOver49Dollars),len(TransactionsUnder49Dollars)],
                         'Avg Sales':                    [TransactionsDataFrame["TRANSACTION_EXT SALES"].mean(),TransactionsOver49Dollars["TRANSACTION_EXT SALES"].mean(),TransactionsUnder49Dollars["TRANSACTION_EXT SALES"].mean() ],
                         'Avg Costs':                    [TransactionsDataFrame['TRANSACTION_EXT COSTS'].mean(),TransactionsOver49Dollars['TRANSACTION_EXT COSTS'].mean(),TransactionsUnder49Dollars['TRANSACTION_EXT COSTS'].mean() ],
                         'Avg InitialMargin':            [TransactionsDataFrame["TRANSACTION_Profit"].mean(),   TransactionsOver49Dollars["TRANSACTION_Profit"].mean(),   TransactionsUnder49Dollars["TRANSACTION_Profit"].mean()  ],
                        
                         
                         'Avg Shipping_Costs':           [TransactionsDataFrame["Shipping_Costs"].mean(),TransactionsOver49Dollars["Shipping_Costs"].mean(),TransactionsUnder49Dollars["Shipping_Costs"].mean() ],
                         'Avg Margin_after_shipping':    [TransactionsDataFrame['Margin_after_shipping'].mean(),TransactionsOver49Dollars['Margin_after_shipping'].mean(),TransactionsUnder49Dollars['Margin_after_shipping'].mean() ],
                         


                         
                         
                         #'Costs': ['Banana', 'Onion', 'Grapes', 'Potato', 'Apple', np.nan, np.nan],
                         })
              
    Summary = Summary.append(SummaryTemp)
    
os.chdir("Z:/P&Ls")
Summary.to_excel("P&L Summary.xlsx")
                                                     
                           
