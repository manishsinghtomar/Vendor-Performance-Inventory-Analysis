import sqlite3
import pandas as pd
import logging
import os
from ingestion_db import ingest_db

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'get_vendor_summary.log')

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler
handler = logging.FileHandler(log_file)
handler.setLevel(logging.DEBUG)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)

def create_vendor_summary(conn) :
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
        SELECT 
           VendorNumber,
           SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
                                             
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price as ActualPrice,
            pp.Volume,
            SUM(p.Quantity) as TotalPurchaseQuantity,
            SUM(p.Dollars) as TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
             ON p.Brand = pp.Brand
        WHERE p.purchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ),
                                             
    SalesSummary AS (
       SELECT
           VendorNo, 
           Brand,
           SUM(SalesQuantity) as TotalSalesQuantity,
           SUM(SalesDollars) as TotalSalesDollars,
           SUM(SalesPrice) as TotalSalesPrice,
           SUM(ExciseTax) as TotalExciseTax
       FROM sales
       GROUP BY VendorNo, Brand 
        )
    
        SELECT  
          ps.VendorNumber,
          ps.VendorName,
          ps.Brand,
          ps.Description,
          ps.PurchasePrice,
          ps.ActualPrice,
          ps.Volume,
          ps.TotalPurchaseQuantity,
          ps.TotalPurchaseDollars,
          ss.TotalSalesQuantity,
          ss.TotalSalesDollars,
          ss.TotalSalesPrice,
          ss.TotalExciseTax,
          fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC""",conn)
    
    return vendor_sales_summary
    
    
    
def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float
    df['Volume'] = df['Volume'].astype( 'float')

    # filling missing value with 0
    df.fillna(0, inplace = True)

    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
   
    return df

if __name__ == '__main__':
    # creating database connection
    conn = sqlite3.connect('inventory.db')

    logger.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(conn)
    logger.info(f'\n{summary_df.head()}')

    logger.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logger.info(f'\n{clean_df.head()}')

    logger.info('Ingesting data....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logger.info('Completed')