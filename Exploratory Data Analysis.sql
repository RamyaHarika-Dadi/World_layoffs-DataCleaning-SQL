-- Exploratory Data Analysis

SELECT * 
FROM world_layoffs.layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- lost all employees
SELECT * 
FROM world_layoffs.layoffs_staging2
where percentage_laid_off = 1;

SELECT * 
FROM world_layoffs.layoffs_staging2
where percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT * 
FROM world_layoffs.layoffs_staging2
where percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, max(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- date coulmn
SELECT MIN(date), MAX(date) 
FROM world_layoffs.layoffs_staging2;

SELECT YEAR(`date`), SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- --------------------------------------------------------------------------------------------------------------------
-- Rolling Totals
-- Total no.of Layoffs Per Month & Year
SELECT substring(`date`,1,7) AS `Month`, sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

-- now we will find the Rolling total Layoffs by using CTE 
WITH Rolling_Total AS
(
SELECT substring(`date`,1,7) AS `month`, sum(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, total_layoffs, SUM(total_layoffs) OVER(ORDER BY `month`) AS Rolling_total
FROM Rolling_Total;

-- -----------------------------------------------------------------------------------------------
-- Now we will look at the companies with the most Layoffs.
SELECT company, SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year.

SELECT company, SUM(total_laid_off) 
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
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