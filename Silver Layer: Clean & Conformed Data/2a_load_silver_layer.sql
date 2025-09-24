------------------------------------------------------------
-- Load silver.cards_data
------------------------------------------------------------
PRINT '--- Starting TRUNCATE and Insert into silver.cards_data ---';
TRUNCATE TABLE silver.cards_data;

DECLARE @StartTime DATETIME = GETDATE();

INSERT INTO silver.cards_data ( 
    id, client_id, card_brand, card_type, card_number, expires, cvv, 
    has_chip, num_cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web
) 
SELECT  
    id, client_id, card_brand, card_type,
    CAST(CAST(card_number AS FLOAT) AS BIGINT) AS card_number,
    TRY_CONVERT(DATE, '01-' + expires, 106) AS expires,
    cvv, has_chip, num_cards_issued,
    credit_limit,
    TRY_CONVERT(DATE, '01-' + acct_open_date, 106) AS acct_open_date,
    YEAR(TRY_CONVERT(DATE, '01-Jan-' + year_pin_last_changed, 106)) AS year_pin_last_changed,
    card_on_dark_web
FROM (
    SELECT id, client_id, card_brand, card_type, card_number, expires, cvv, has_chip, num_cards_issued,
           TRIM(SUBSTRING(concate1,2,CHARINDEX(',', concate1)-2)) AS credit_limit,
           TRIM(SUBSTRING(concate2,CHARINDEX(',', concate2)+1,CHARINDEX(',', concate2)+6)) AS acct_open_date,
           TRIM(SUBSTRING(concate3,CHARINDEX('|', concate3)+1,4)) AS year_pin_last_changed,
           card_on_dark_web
    FROM (
        SELECT id, client_id, card_brand, card_type, card_number, expires, cvv, has_chip, num_cards_issued,
               REPLACE(REPLACE([credit_limit]+[acct_open_date],'"',''),' ',',') AS concate1,
               REPLACE(REPLACE([acct_open_date]+[year_pin_last_changed],'"',''),' ',',') AS concate2,
               CASE 
                   WHEN LEN(year_pin_last_changed) = 4 THEN year_pin_last_changed
                   ELSE ([year_pin_last_changed]+'|'+[card_on_dark_web]) 
               END AS concate3,
               'NO' AS card_on_dark_web
        FROM [Financial Transaction WHD].[bronze].[cards_data]
    ) t
) b
WHERE acct_open_date > year_pin_last_changed;

DECLARE @EndTime DATETIME = GETDATE();
PRINT 'Inserted into silver.cards_data in ' + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR) + ' seconds.';

SELECT COUNT(*) AS InsertedRows FROM silver.cards_data;



------------------------------------------------------------
-- Load silver.users_data
------------------------------------------------------------
PRINT '--- Starting Insert into silver.users_data ---';
DECLARE @StartTime2 DATETIME = GETDATE();

INSERT INTO [Financial Transaction WHD].[silver].[users_data] ( 
      id, current_age, retirement_age, birth_year, birth_month, gender, address, 
      latitude, longitude, per_capita_income, yearly_income, total_debt, credit_score, num_credit_cards
) 
SELECT   
      id, current_age, retirement_age, birth_year, birth_month, gender, address, 
      latitude, longitude,
      CAST(REPLACE(REPLACE([per_capita_income], '$', ''), ',', '') AS INT),
      CAST(REPLACE(REPLACE([yearly_income], '$', ''), ',', '') AS INT),
      CAST(REPLACE(REPLACE([total_debt], '$', ''), ',', '') AS INT),
      credit_score, num_credit_cards
FROM [Financial Transaction WHD].[bronze].[users_data];

DECLARE @EndTime2 DATETIME = GETDATE();
PRINT 'Inserted into silver.users_data in ' + CAST(DATEDIFF(SECOND, @StartTime2, @EndTime2) AS VARCHAR) + ' seconds.';

SELECT COUNT(*) AS InsertedRows FROM [Financial Transaction WHD].[silver].[users_data];



------------------------------------------------------------
-- Load silver.mcc_codes
------------------------------------------------------------
PRINT '--- Starting Insert into silver.mcc_codes ---';
DECLARE @StartTime3 DATETIME = GETDATE();

INSERT INTO [Financial Transaction WHD].[silver].[mcc_codes] ( 
    mcc_code, description
) 
SELECT  
    CAST([mcc_code] AS INT), description
FROM [Financial Transaction WHD].[bronze].[mcc_codes_json];

DECLARE @EndTime3 DATETIME = GETDATE();
PRINT 'Inserted into silver.mcc_codes in ' + CAST(DATEDIFF(SECOND, @StartTime3, @EndTime3) AS VARCHAR) + ' seconds.';

SELECT COUNT(*) AS InsertedRows FROM [Financial Transaction WHD].[silver].[mcc_codes];



------------------------------------------------------------
-- Load silver.train_fraud_labels
------------------------------------------------------------
PRINT '--- Starting Insert into silver.train_fraud_labels ---';
DECLARE @StartTime4 DATETIME = GETDATE();

INSERT INTO [Financial Transaction WHD].[silver].[train_fraud_labels] (transaction_id, target) 
SELECT  
    transaction_id,
    CASE  
        WHEN target IN ('Yes', 'Fraud', '1') THEN 1
        WHEN target IN ('No', 'Not Fraud', '0') THEN 0
        ELSE NULL
    END AS target
FROM [Financial Transaction WHD].[bronze].[train_fraud_labels];

DECLARE @EndTime4 DATETIME = GETDATE();
PRINT 'Inserted into silver.train_fraud_labels in ' + CAST(DATEDIFF(SECOND, @StartTime4, @EndTime4) AS VARCHAR) + ' seconds.';

SELECT COUNT(*) AS InsertedRows, 
       COUNT(CASE WHEN target IS NULL THEN 1 END) AS NullTargets
FROM [Financial Transaction WHD].[silver].[train_fraud_labels];



------------------------------------------------------------
-- Load silver.transactions_data via BULK INSERT
------------------------------------------------------------
PRINT '--- Starting BULK INSERT into silver.transactions_data ---';
DECLARE @StartTime5 DATETIME = GETDATE();

BULK INSERT [Financial Transaction WHD].[silver].[transactions_data] 
FROM '/home/m.farrag/transactions_data.csv' 
WITH ( 
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

DECLARE @EndTime5 DATETIME = GETDATE();
PRINT 'Bulk inserted into silver.transactions_data in ' + CAST(DATEDIFF(SECOND, @StartTime5, @EndTime5) AS VARCHAR) + ' seconds.';

SELECT COUNT(*) AS InsertedRows FROM [Financial Transaction WHD].[silver].[transactions_data];
