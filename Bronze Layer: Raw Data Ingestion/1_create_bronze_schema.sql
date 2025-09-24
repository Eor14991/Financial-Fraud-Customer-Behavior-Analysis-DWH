------------------------------------------------------------
-- Create schema
------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze;');
GO


------------------------------------------------------------
-- Users Data Table
------------------------------------------------------------
IF OBJECT_ID('bronze.users_data','U') IS NOT NULL
    DROP TABLE bronze.users_data;
CREATE TABLE bronze.users_data (
    id NVARCHAR(MAX),
    current_age NVARCHAR(MAX),
    retirement_age NVARCHAR(MAX),
    birth_year NVARCHAR(MAX),
    birth_month NVARCHAR(MAX),
    gender NVARCHAR(MAX),
    address NVARCHAR(MAX),
    latitude NVARCHAR(MAX),
    longitude NVARCHAR(MAX),
    per_capita_income NVARCHAR(MAX),
    yearly_income NVARCHAR(MAX),
    total_debt NVARCHAR(MAX),
    credit_score NVARCHAR(MAX),
    num_credit_cards NVARCHAR(MAX)
);
GO


------------------------------------------------------------
-- Cards Data Table
------------------------------------------------------------
IF OBJECT_ID('bronze.cards_data','U') IS NOT NULL
    DROP TABLE bronze.cards_data;
CREATE TABLE bronze.cards_data (
    id NVARCHAR(MAX),
    client_id NVARCHAR(MAX),
    card_brand NVARCHAR(MAX),
    card_type NVARCHAR(MAX),
    card_number NVARCHAR(MAX),
    expires NVARCHAR(MAX),
    cvv NVARCHAR(MAX),
    has_chip NVARCHAR(MAX),
    num_cards_issued NVARCHAR(MAX),
    credit_limit NVARCHAR(MAX),
    acct_open_date NVARCHAR(MAX),
    year_pin_last_changed NVARCHAR(MAX),
    card_on_dark_web NVARCHAR(MAX)
);
GO


------------------------------------------------------------
-- Transactions Data Table
------------------------------------------------------------
IF OBJECT_ID('bronze.transactions_data','U') IS NOT NULL
    DROP TABLE bronze.transactions_data;
CREATE TABLE bronze.transactions_data (
    id NVARCHAR(MAX),
    date NVARCHAR(MAX),
    client_id NVARCHAR(MAX),
    card_id NVARCHAR(MAX),
    amount NVARCHAR(MAX),
    use_chip NVARCHAR(MAX),
    merchant_id NVARCHAR(MAX),
    merchant_city NVARCHAR(MAX),
    merchant_state NVARCHAR(MAX),
    zip NVARCHAR(MAX),
    mcc NVARCHAR(MAX),
    errors NVARCHAR(MAX)
);
GO


------------------------------------------------------------
-- MCC Codes Table
------------------------------------------------------------
IF OBJECT_ID('bronze.mcc_codes_json','U') IS NOT NULL
    DROP TABLE bronze.mcc_codes_json;
CREATE TABLE bronze.mcc_codes_json (
    mcc_code NVARCHAR(10) PRIMARY KEY,
    description NVARCHAR(255)
);
GO


------------------------------------------------------------
-- Fraud Labels Table
------------------------------------------------------------
IF OBJECT_ID('bronze.train_fraud_labels','U') IS NOT NULL
    DROP TABLE bronze.train_fraud_labels;
CREATE TABLE bronze.train_fraud_labels (
    transaction_id BIGINT PRIMARY KEY,
    target NVARCHAR(10)
);
GO
