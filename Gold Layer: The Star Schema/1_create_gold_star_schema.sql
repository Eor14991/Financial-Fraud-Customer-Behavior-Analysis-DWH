-- Create a dedicated schema for the Data Warehouse Gold Layer if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO


-- Drop Fact Table first (because it has FKs)
IF OBJECT_ID('gold.FactTransactions', 'U') IS NOT NULL
    DROP TABLE gold.FactTransactions;
GO

-- Drop Dimension Tables
IF OBJECT_ID('gold.DimUser', 'U') IS NOT NULL
    DROP TABLE gold.DimUser;
GO

IF OBJECT_ID('gold.DimCard', 'U') IS NOT NULL
    DROP TABLE gold.DimCard;
GO

IF OBJECT_ID('gold.DimMerchant', 'U') IS NOT NULL
    DROP TABLE gold.DimMerchant;
GO

IF OBJECT_ID('gold.DimDate', 'U') IS NOT NULL
    DROP TABLE gold.DimDate;
GO


-----------------------------------------------------
-- DIMENSION TABLE: DimUser
-- Creates a physical table for user attributes, using the logic from your customer view.
-----------------------------------------------------
CREATE TABLE gold.DimUser (
    UserKey INT PRIMARY KEY,         -- This is the new, efficient key for joining
    OriginalUserID INT,              -- The ID from the source system for reference
    BirthYear INT,
    Gender VARCHAR(10),
    YearlyIncome DECIMAL(18, 2),
    TotalDebt DECIMAL(18, 2),
    CreditScore INT,
    NumCreditCards INT
);
GO

-----------------------------------------------------
-- DIMENSION TABLE: DimCard
-- Creates a physical table for card attributes.
-- Note: We are selecting only the columns relevant for analysis.
-----------------------------------------------------
CREATE TABLE gold.DimCard (
    CardKey INT PRIMARY KEY,         -- New key for joining
    CardBrand VARCHAR(20),
    CardType VARCHAR(50),
    HasChip varchar(20),
    CreditLimit DECIMAL(18, 2)
);
GO

-----------------------------------------------------
-- DIMENSION TABLE: DimMerchant
-- Creates a physical, unique list of merchants with a new surrogate key.
-----------------------------------------------------
CREATE TABLE gold.DimMerchant (
    MerchantKey INT IDENTITY(1,1) PRIMARY KEY, -- A new, auto-incrementing key for efficiency
    SourceMerchantID INT,                      -- The original merchant ID
    MerchantCity VARCHAR(100),
    MerchantState VARCHAR(10),
    MerchantCategory VARCHAR(255)              -- Enriched with the MCC description
);
GO

-----------------------------------------------------
-- DIMENSION TABLE: DimDate
-- A standard data warehouse table for powerful time-based analysis.
-- This table should be pre-populated with all dates for several years.
-----------------------------------------------------
CREATE TABLE gold.DimDate (
    DateKey INT PRIMARY KEY, -- e.g., 20230115
    FullDate DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    DayOfWeek INT NOT NULL,
    IsWeekend BIT NOT NULL
);
GO

-----------------------------------------------------
-- FACT TABLE: FactTransactions
-- The central table with numerical measures and integer foreign keys.
-----------------------------------------------------
CREATE TABLE gold.FactTransactions (
    -- Foreign Keys to the Dimension tables
    DateKey INT FOREIGN KEY REFERENCES gold.DimDate(DateKey),
    UserKey INT FOREIGN KEY REFERENCES gold.DimUser(UserKey),
    CardKey INT FOREIGN KEY REFERENCES gold.DimCard(CardKey),
    MerchantKey INT FOREIGN KEY REFERENCES gold.DimMerchant(MerchantKey),

    -- Degenerate Dimension (the original transaction ID for reference)
    TransactionID BIGINT,

    -- Measures (the quantitative values we want to analyze)
    Amount DECIMAL(18, 2),
    IsFraud BIT
);
GO
