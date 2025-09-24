CREATE OR ALTER PROCEDURE bronze.load_bronze_layer
AS
BEGIN
    -- This procedure truncates and reloads all bronze layer tables from source files.

    SET NOCOUNT ON; -- Prevents sending back rowcount messages.

    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @total_start_time DATETIME, @total_end_time DATETIME;
    
    SET @total_start_time = GETDATE();
    
    PRINT '==================================================';
    PRINT 'Starting Bronze Layer Load';
    PRINT '==================================================';

    BEGIN TRY
        -----------------------------------------------------
        -- Load mcc_codes.json
        -----------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.mcc_codes_json;

        INSERT INTO bronze.mcc_codes_json (mcc_code, description)
        SELECT [key], value
        FROM OPENROWSET(
            BULK '/home/m.farrag/Desktop/archive (2)/mcc_codes.json',
            SINGLE_CLOB
        ) AS j
        CROSS APPLY OPENJSON(BulkColumn);


        



        SET @end_time = GETDATE();
        PRINT '>>>>> Load Duration: ' + CONVERT(VARCHAR(10), DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds';
        PRINT '-----------------------------------------------------';

        -----------------------------------------------------
        -- Load train_fraud_labels.json
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.train_fraud_labels';
        TRUNCATE TABLE bronze.train_fraud_labels;

        PRINT '>> Inserting into Table bronze.train_fraud_labels';

        INSERT INTO bronze.train_fraud_labels (transaction_id, target)
        SELECT TRY_CAST([key] AS BIGINT), value
        FROM OPENROWSET(
            BULK '/home/m.farrag/Desktop/archive (2)/train_fraud_labels.json',
            SINGLE_CLOB
        ) AS j
        CROSS APPLY OPENJSON(BulkColumn, '$.target');


        SET @end_time = GETDATE();
        PRINT '>>>>> Load Duration: ' + CONVERT(VARCHAR(10), DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds';
        PRINT '-----------------------------------------------------';

        -----------------------------------------------------
        -- Load users_data.csv
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.users_data';
        TRUNCATE TABLE bronze.users_data;

        PRINT '>> Inserting into Table bronze.users_data';
        BULK INSERT bronze.users_data
        FROM '/home/m.farrag/Desktop/archive (2)/users_data.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>>>> Load Duration: ' + CONVERT(VARCHAR(10), DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds';
        PRINT '-----------------------------------------------------';

        -----------------------------------------------------
        -- Load cards_data.csv
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze.cards_data';
        TRUNCATE TABLE bronze.cards_data;

        PRINT '>> Inserting into Table bronze.cards_data';
        BULK INSERT bronze.cards_data
        FROM '/home/m.farrag/Desktop/archive (2)/cards_data.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>>>> Load Duration: ' + CONVERT(VARCHAR(10), DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds';
        PRINT '-----------------------------------------------------';

        -----------------------------------------------------
        -- Load transactions_data.csv
        -----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table bronze';
        TRUNCATE TABLE bronze.transactions_data;

        PRINT '>> Inserting into Table bronze.transactions_data';
        BULK INSERT bronze.transactions_data
        FROM '/home/m.farrag/Desktop/archive (2)/transactions_data.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>>>> Load Duration: ' + CONVERT(VARCHAR(10), DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds';
        PRINT '-----------------------------------------------------';

        SET @total_end_time = GETDATE();
        PRINT '==================================================';
        PRINT 'Finished Bronze Layer Load Successfully';
        PRINT '>>>>> Total Load Duration: ' + CONVERT(VARCHAR(10), DATEDIFF(SECOND, @total_start_time, @total_end_time)) + ' seconds';
        PRINT '==================================================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================';
        PRINT 'ERROR IN LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT '=========================================';
    END CATCH;
END;
GO

-- Execute the procedure
EXEC bronze.load_bronze_layer;
GO
