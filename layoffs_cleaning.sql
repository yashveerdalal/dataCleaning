
USE world_layoffs;

CREATE TABLE layoffs_staging (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT,
  source_url TEXT
);

SET GLOBAL local_infile = 1;
LOAD DATA LOCAL INFILE '/Users/yashdalal/Desktop/layoffs.csv'
INTO TABLE layoffs_staging
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

WITH numbered_data AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
           ORDER BY company
         ) AS flagged_dupes
  FROM layoffs_staging
)
SELECT * FROM numbered_data WHERE flagged_dupes > 1;

CREATE TABLE layoffs_cleaned (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT,
  source_url TEXT,
  serial_number INT
);

INSERT INTO layoffs_cleaned (
  company, location, industry, total_laid_off,
  percentage_laid_off, `date`, stage, country, funds_raised_millions, serial_number
)
SELECT 
  company, location, industry, total_laid_off,
  percentage_laid_off, `date`, stage, country, funds_raised_millions,
  ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, total_laid_off,
                 percentage_laid_off, `date`, stage, country, funds_raised_millions
    ORDER BY company
  ) AS serial_number
FROM layoffs_staging;

DELETE FROM layoffs_cleaned
WHERE serial_number > 1;

SELECT DISTINCT company, TRIM(company) FROM layoffs_cleaned;

UPDATE layoffs_cleaned
SET company = TRIM(company);

SELECT DISTINCT industry FROM layoffs_cleaned ORDER BY 1;

SELECT *
FROM layoffs_cleaned
WHERE industry LIKE 'Crypto%' ;

UPDATE layoffs_cleaned
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT *
FROM layoffs_cleaned
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_cleaned
ORDER BY 1;

UPDATE layoffs_cleaned
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%' ;

SELECT `date`
FROM layoffs_cleaned
ORDER BY 1
LIMIT 10;

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS proper_date
FROM layoffs_cleaned
LIMIT 20;

UPDATE layoffs_cleaned
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_cleaned
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_cleaned
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_cleaned
WHERE industry IS NULL
OR industry = '';

SELECT
    t1.company,
    t1.industry AS missing_industry,
    t2.industry AS filled_industry
FROM layoffs_cleaned t1
JOIN layoffs_cleaned t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;

UPDATE layoffs_cleaned
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_cleaned t1
JOIN layoffs_cleaned t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_cleaned
WHERE company = "Bally's Interactive" AND industry IS NOT NULL;

UPDATE layoffs_cleaned
SET industry = 'Gaming'
WHERE company = "Bally's Interactive" AND industry IS NULL;

SELECT *
FROM layoffs_cleaned
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_cleaned
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_cleaned
DROP COLUMN serial_number;

SELECT * FROM layoffs_cleaned;
