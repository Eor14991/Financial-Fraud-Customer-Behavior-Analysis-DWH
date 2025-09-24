
CREATE SCHEMA silver


CREATE TABLE silver.cards_data(
    id varchar(50),
    client_id varchar(50),
    card_brand NVARCHAR(50),
    card_type NVARCHAR(50),
    card_number VARCHAR(20),
    expires Date,
    cvv INT,
    has_chip NVARCHAR(10),
    num_cards_issued INT,
    credit_limit DECIMAL(18,2),
    acct_open_date DATE,
    year_pin_last_changed int,
    card_on_dark_web NVARCHAR(10)
);
GO

CREATE TABLE [silver].[users_data] (
    id INT,
    current_age INT,
    retirement_age INT,
    birth_year INT,
    birth_month INT,
    gender NVARCHAR(10),
    address NVARCHAR(255),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    per_capita_income INT,
    yearly_income INT,
    total_debt INT,
    credit_score INT,
    num_credit_cards INT
);
GO


TRUNCATE table [silver].[mcc_codes]
CREATE TABLE [Financial Transaction WHD].[silver].[mcc_codes] (
    mcc_code INT,
    description NVARCHAR(255)
);
GO

CREATE TABLE [Financial Transaction WHD].[silver].[train_fraud_labels] (
    transaction_id BIGINT,
    target NVARCHAR(10)
);


CREATE TABLE [silver].[transactions_data] (
    id VARCHAR(50),
    date DATETIME,
    client_id VARCHAR(50),
    card_id VARCHAR(50),
    amount DECIMAL,
    use_chip NVARCHAR(20),
    merchant_id NVARCHAR(30),
    merchant_city NVARCHAR(100),
    merchant_state NVARCHAR(50),
    zip NVARCHAR(20),
    mcc NVARCHAR(10),
    errors NVARCHAR(MAX)
);
