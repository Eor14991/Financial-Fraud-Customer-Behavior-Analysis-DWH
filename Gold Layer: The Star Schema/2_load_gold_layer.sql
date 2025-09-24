BEGIN TRY
    PRINT '=== START TRANSACTION ===';
    BEGIN TRANSACTION;

    -----------------------------------------------------
    -- STEP 1: Load Dimension Table - DimUser
    -----------------------------------------------------
    PRINT 'Step 1: Loading DimUser...'; 

    INSERT INTO gold.DimUser (UserKey, OriginalUserID, BirthYear, Gender, YearlyIncome, TotalDebt, CreditScore, NumCreditCards) 
    SELECT DISTINCT
        TRY_CAST(id AS INT) AS UserKey, 
        TRY_CAST(id AS INT) AS OriginalUserID, 
        TRY_CAST(birth_year AS INT), 
        gender, 
        yearly_income, 
        total_debt, 
        credit_score, 
        num_credit_cards 
    FROM [Financial Transaction WHD].[silver].[users_data]; 

    DECLARE @DimUserCount INT = (SELECT COUNT(*) FROM gold.DimUser);
    PRINT 'Step 1 completed: ' + CAST(@DimUserCount AS VARCHAR(10)) + ' rows loaded into DimUser.';

    -----------------------------------------------------
    -- STEP 2: Load Dimension Table - DimCard
    -----------------------------------------------------
    PRINT 'Step 2: Loading DimCard...';

    INSERT INTO gold.DimCard (CardKey, CardBrand, CardType, HasChip, CreditLimit)
    SELECT DISTINCT
        TRY_CAST(id AS INT) AS CardKey,
        card_brand,
        card_type,
        CASE WHEN UPPER(has_chip) = 'YES' THEN 1 ELSE 0 END AS HasChip,
        credit_limit
    FROM [Financial Transaction WHD].[silver].[cards_data];

    DECLARE @DimCardCount INT = (SELECT COUNT(*) FROM gold.DimCard);
    PRINT 'Step 2 completed: ' + CAST(@DimCardCount AS VARCHAR(10)) + ' rows loaded into DimCard.';

    -----------------------------------------------------
    -- STEP 3: Load Dimension Table - DimMerchant
    -----------------------------------------------------
    PRINT 'Step 3: Loading DimMerchant...';

    INSERT INTO gold.DimMerchant (SourceMerchantID, MerchantCity, MerchantState, MerchantCategory)
    SELECT DISTINCT 
        TRY_CAST(t.merchant_id AS INT) AS SourceMerchantID,
        t.merchant_city,
        t.merchant_state,
        m.description AS MerchantCategory
    FROM [Financial Transaction WHD].[silver].[transactions_data] t
    LEFT JOIN [Financial Transaction WHD].[silver].[mcc_codes] m ON TRY_CAST(t.mcc AS INT) = m.mcc_code
    WHERE t.merchant_id IS NOT NULL;

    DECLARE @DimMerchantCount INT = (SELECT COUNT(*) FROM gold.DimMerchant);
    PRINT 'Step 3 completed: ' + CAST(@DimMerchantCount AS VARCHAR(10)) + ' rows loaded into DimMerchant.';

    -----------------------------------------------------
    -- STEP 4: Load Fact Table - FactTransactions
    -----------------------------------------------------
    PRINT 'Step 4: Loading FactTransactions...';

    INSERT INTO gold.FactTransactions (DateKey, UserKey, CardKey, MerchantKey, TransactionID, Amount, IsFraud)
    SELECT 
        CONVERT(INT, CONVERT(VARCHAR(8), t.date, 112)) AS DateKey,
        TRY_CAST(t.client_id AS INT) AS UserKey,
        TRY_CAST(t.card_id AS INT) AS CardKey,
        ISNULL(m.MerchantKey, -1) AS MerchantKey,
        t.id AS TransactionID,
        t.amount,
        CASE 
            WHEN UPPER(f.target) = 'YES' THEN 1
            WHEN UPPER(f.target) = 'NO' THEN 0
            ELSE NULL
        END AS IsFraud
    FROM [Financial Transaction WHD].[silver].[transactions_data] t
    LEFT JOIN [Financial Transaction WHD].[silver].[train_fraud_labels] f ON t.id = f.id
    LEFT JOIN gold.DimMerchant m ON TRY_CAST(t.merchant_id AS INT) = m.SourceMerchantID;

    DECLARE @FactCount INT = (SELECT COUNT(*) FROM gold.FactTransactions);
    PRINT 'Step 4 completed: ' + CAST(@FactCount AS VARCHAR(10)) + ' rows loaded into FactTransactions.';

    COMMIT TRANSACTION;
    PRINT '=== TRANSACTION COMMITTED SUCCESSFULLY ===';

END TRY
BEGIN CATCH
    PRINT '*** ERROR OCCURRED ***';
    PRINT ERROR_MESSAGE();
    ROLLBACK TRANSACTION;
    PRINT 'Transaction rolled back.';
END CATCH;
