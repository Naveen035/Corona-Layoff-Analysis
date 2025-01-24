use first_project;

SELECT 
    *
FROM
    first_project.layoffs;

-- duplicate the table

CREATE TABLE first_project.layoffs_copy LIKE first_project.layoffs;

insert layoffs_copy select * from first_project.layoffs;

SELECT 
    *
FROM
    first_project.layoffs_copy;

-- Removing the Duplicates

select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) from first_project.layoffs_copy;

select * from (select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) as row_num from first_project.layoffs_copy
) as duplicates where row_num > 1;

-- where the rows having greater 1 are duplicates

CREATE TABLE `first_project`.`layoffs_copy2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT,
    row_num INT
);

SELECT 
    *
FROM
    first_project.layoffs_copy2;

insert into layoffs_copy2
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) from first_project.layoffs_copy;

SELECT 
    *
FROM
    first_project.layoffs_copy2
WHERE
    row_num > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM first_project.layoffs_copy2 
WHERE
    row_num > 1;

-- we deleted the duplicates

-- Standarize the data

UPDATE layoffs_copy2 
SET 
    company = TRIM(company);

SELECT DISTINCT
    industry
FROM
    first_project.layoffs_copy2
WHERE
    industry LIKE 'Crypto%';

UPDATE layoffs_copy2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';

SELECT DISTINCT
    location
FROM
    first_project.layoffs_copy2
ORDER BY 1;

SELECT DISTINCT
    country
FROM
    first_project.layoffs_copy2
ORDER BY 1;

SELECT DISTINCT
    (country)
FROM
    first_project.layoffs_copy2
WHERE
    country LIKE 'United S%';

UPDATE layoffs_copy2 
SET 
    country = 'United States'
WHERE
    country LIKE 'United S%';

SELECT 
    `date`
FROM
    first_project.layoffs_copy2;

SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS new_date
FROM
    first_project.layoffs_copy2;

UPDATE layoffs_copy2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

alter table layoffs_copy2 modify column `date` Date;

-- removing null values and empty spaces 

SELECT DISTINCT
    industry
FROM
    first_project.layoffs_copy2
ORDER BY industry;

SELECT 
    *
FROM
    first_project.layoffs_copy2
WHERE
    industry IS NULL OR industry = ''
ORDER BY industry;

UPDATE layoffs_copy2 
SET 
    industry = NULL
WHERE
    industry = '';

SELECT 
    t1.industry, t2.industry
FROM
    first_project.layoffs_copy2 t1
        JOIN
    first_project.layoffs_copy2 t2 ON t1.company = t2.company
        AND t1.location = t2.location
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;

UPDATE layoffs_copy2 t1
        JOIN
    layoffs_copy2 t2 ON t1.location = t2.location 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;

SELECT 
    *
FROM
    first_project.layoffs_copy2;

SELECT 
    *
FROM
    first_project.layoffs_copy2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

alter table layoffs_copy2 drop row_num;

DELETE FROM first_project.layoffs_copy2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- EDA (Exploratory Data Analysis)

SELECT 
    SUM(total_laid_off)
FROM
    first_project.layoffs_copy2;

SELECT 
    MAX(total_laid_off)
FROM
    first_project.layoffs_copy2;

SELECT 
    MIN(total_laid_off)
FROM
    first_project.layoffs_copy2;

SELECT 
    company, total_laid_off
FROM
    first_project.layoffs_copy2
WHERE
    total_laid_off IS NOT NULL
GROUP BY company , total_laid_off
ORDER BY total_laid_off DESC;

SELECT 
    MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM
    first_project.layoffs_copy2
WHERE
    percentage_laid_off IS NOT NULL;
    
-- Which companies had 1 which is basically 100 percent of they company laid off

SELECT 
    *
FROM
    first_project.layoffs_copy2
WHERE
    percentage_laid_off = 1
order by funds_raised_millions desc;

-- top 5 companies which had laid off more employees
SELECT 
    company, total_laid_off
FROM
    first_project.layoffs_copy2
ORDER BY 2 DESC
LIMIT 5;

-- Top 10 Companies with the most number of layoffs

SELECT 
    company, SUM(total_laid_off)
FROM
    first_project.layoffs_copy2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- based on top 5 locations

SELECT 
    location, SUM(total_laid_off)
FROM
    first_project.layoffs_copy2
GROUP BY location
ORDER BY 2 DESC
LIMIT 5;

-- based on Year

SELECT 
    YEAR(`date`) AS `Year`, SUM(total_laid_off)
FROM
    first_project.layoffs_copy2
GROUP BY `Year`;

-- Based on top 5 industries

SELECT 
    industry, SUM(total_laid_off)
FROM
    first_project.layoffs_copy2
GROUP BY industry
ORDER BY 2 DESC
LIMIT 5;

-- Based on the Stages

SELECT 
    stage, SUM(total_laid_off)
FROM
    first_project.layoffs_copy2
GROUP BY stage
ORDER BY 2 DESC;

-- Ranking based on the company

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM first_project.layoffs_copy2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

SELECT 
    SUBSTRING(`date`, 1, 7) AS dates, SUM(total_laid_off)
FROM
    first_project.layoffs_copy2
GROUP BY dates
ORDER BY dates ASC;

-- cumulative sum of layoffs by every month

with cumsum_cte as (
SELECT 
    SUBSTRING(`date`, 1, 7) AS dates, SUM(total_laid_off) as total_laid_off
FROM
    first_project.layoffs_copy2
GROUP BY dates
ORDER BY dates ASC
)
select dates,sum(total_laid_off) over(order by dates asc) as cumsum
from cumsum_cte order by dates asc



