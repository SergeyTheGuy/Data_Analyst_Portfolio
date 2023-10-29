-- Acknowledgement
-- The present dataset was retrieved from: https://www.kaggle.com/datasets/matthieugimbert/french-bakery-daily-sales


USE prj_frenchbaker;
-- ############### I. Importing the dataset ###############

-- This step has been performed using Python

-- ############### II. Data cleaning ###############
SELECT *
FROM sales;

-- ###### 1. Processing the 'id' column ######
-- Having a closer look at the datase, it appears that "id" column does not really convey any useful information:
-- 		the ids are not sequential, they do not represent individual customers (this role belongs to the ticket_number),
-- 		they do not relate to other values in any way;
-- 		Although, it is apparent that the id column is pretty much redundant, it still remains a useful atribute since
-- 		every id represents an individual transaction. Ids can be used to perform the dataset normalisation firstly,
-- 		and in the end can be discarded.

ALTER TABLE sales
DROP COLUMN id;

ALTER TABLE sales
ADD COLUMN id INT PRIMARY KEY AUTO_INCREMENT FIRST;

-- ###### 2. Processing the ticket_number column ######
-- On the other hand, a ticket number seems to represent a unique customer.
-- 		Though repeated, but every ticket number has identical dates and time, but differs in product article;
-- 		therefore, the combination of the ticket_number, date, time, and article columns 
-- 		can be considered as the compound primary key in the scope of this dataset.
-- 		Although it is logical to keep ticket numbers as integers, but it is a sort of id that represents a unique customer,
-- 		and won't be used in further calculations as a measurement argument. Hence, it can be recorded as VARCHAR.

ALTER TABLE sales
CHANGE ticket_number ticket_number VARCHAR(50) NOT NULL AFTER id;

-- ###### 3. Processing the 'quantity' column ######
SELECT quantity
FROM sales
WHERE quantity <= 0;

-- It appears that there are some negative quantity values, as well as 0.
-- 		Zero values seem to be associated with free give-aways.
-- 		In turn, according to the author of the dataset, negative quantities stand for refund, or are the result of till-typo.
-- 		Since it is impossible to identify what values are real refunds or typo erros, in the scope of the present project
--    	1/3 of negative quantity values will be randomly sampled and treated as typo errors,
--    	whereas the rest 2/3 will be considered as true refunds.
-- It is also assumed that one ticket cannot contain operations for both pay and refund at the same time,
--    	so negative quantity values are sampled based on the unique ticket number.

-- ### 3.1 Randomly sampling 1/3 of distinct ticket numbers where quantity < 0,
--    	wraping inside a temporary table
DROP TEMPORARY TABLE IF EXISTS tmp;

CREATE TEMPORARY TABLE tmp (
SELECT sample_ticket
FROM (
	SELECT DISTINCT(ticket_number) AS sample_ticket, ROW_NUMBER() OVER (ORDER BY ticket_number) AS row_numb
	FROM sales
	WHERE quantity < 0
	ORDER BY RAND()
) AS subq1
WHERE row_numb <= (
	SELECT COUNT(DISTINCT(ticket_number)) DIV 3 
    FROM sales 
    WHERE quantity < 0)
);

-- ### 3.2 Create another temporary table, which is joined with the one created above. 
-- 		The column containing new quantity values is added.  
--      For the sampled tickets, negative quantities are being converted into positive.
-- 		Meanwhile, not transformed quantities are kept as in the original column.

DROP TEMPORARY TABLE IF EXISTS tmp2;

CREATE TEMPORARY TABLE tmp2 (
SELECT *,
	CASE 
		WHEN s.ticket_number = tmp.sample_ticket THEN s.quantity - (s.quantity*2)
        ELSE s.quantity
	END AS new_quant
FROM sales s
LEFT JOIN tmp
	ON s.ticket_number = tmp.sample_ticket
);

-- ### 3.3 Add a new column to the original dataset to populate with new quantity
ALTER TABLE sales
ADD COLUMN new_quantity INT AFTER quantity;

-- ### 3.4 Populating the column with new quantity values
UPDATE sales s
LEFT JOIN tmp2
	ON s.id = tmp2.id
SET s.new_quantity = tmp2.new_quant
WHERE s.id = tmp2.id;

-- ### 3.5 Check if quantity values were altered correctly
SELECT subq1.checking 
FROM (
SELECT *,
	CASE
		WHEN quantity = new_quantity THEN TRUE
        WHEN new_quantity = quantity - quantity*2 THEN TRUE
        ELSE FALSE
	END AS checking
FROM sales) AS subq1
WHERE subq1.checking = 0;
-- Check result: there were no FALSE values, SO everything was altered correctly

-- ### 3.6 Droping the original quantity column
ALTER TABLE sales 
DROP COLUMN quantity;

ALTER TABLE sales
RENAME COLUMN new_quantity TO quantity;

-- ###### 4. Processing the 'unit_price' column ######
-- The "unit_price" column is currently in the string format, comma is the curent type of delimiter, currency signs are found at the back;
-- 		the column must be in the float format, with dot as delimiter, without currency tags.

-- ### 4.1 Creating a CTE that containig normalised prices
-- 		and updating the original ones in the dataset
WITH price_fixed AS (
	SELECT id, 
		CAST(REPLACE(RTRIM(REPLACE(unit_price, '€', '')), ',', '.') AS DECIMAL(6, 2)) AS cleaned_price
	FROM sales
)
UPDATE sales s
JOIN price_fixed pf
	ON s.id = pf.id
SET s.unit_price = pf.cleaned_price;

-- ### 4.2 Renaming the column to preserve the currency information
ALTER TABLE sales
RENAME COLUMN unit_price TO unit_price_euro;


-- ######  5. Adding total column ######
-- The "unit_price" column represents a price of a particular product for a piece regardless quantity.
-- 		A result column of unit_price multiplied by quantity can tell how much 
-- 		a customer paid for certain item in total.	

-- ### 5.1 Adding a new column
ALTER TABLE sales
ADD COLUMN total_price_per_unit DECIMAL(10, 2) DEFAULT NULL AFTER unit_price_euro;

-- ### 5.2 Populating with new values
UPDATE sales
SET total_price_per_unit = quantity*unit_price_euro;

-- ###### 6. Total price per ticket ######
-- Going further, since there are repeating ticket numbers, it means that more likely every unique ticket
-- 		represents a customer, and, in some cases, has multiple purchases.
-- 		Hence, after finding a total value of purchase per individual unit ('total_price_per_unit' column),
-- 		the amount of total purchase per unique ticket (i.e. per customer) can be obtained ('total_price_per_ticket' column)
-- 		as the sum of total prices per unit for tickets with the same number.
-- 		To meet the requirements of 3NF normalisation, a new table has to be created that would contain
-- 		unique ticket numbers and the amounts of total purchase per every ticket.

-- ### 6.1 Crreating a new table
CREATE TABLE total_ticket_purchase (
SELECT ticket_number,
	SUM(total_price_per_unit) AS total_price_per_ticket
FROM sales
GROUP BY ticket_number
);

-- ###### 7. Triggers ######
-- There have been two additional columns calculated as derivatives of quantity and unit_price: 
-- 		total_price_per_unit, which equals quantity times unit_price_euro,
-- 		and total_price_per_ticket (total_ticket_purchase table), which is the sum of total_price_per_unit per unique ticket.
-- 		In order to make sure that even when quantity or unit price values are updated,
-- 		the calculations get updated automatically, too.
-- 		To achieve this goal, a sequence of triggers must be created.

-- ### 7.1 Temporary logs 
-- Create a trigger, which will create two temporary tables:
-- 		one that keeps track of updated ticket numbers, and the other
-- 		containes a flag that equals 1 (required for another trigger).

DELIMITER //
CREATE TRIGGER updated_tickets_and_flag
BEFORE UPDATE ON sales
FOR EACH ROW
creating_tables:BEGIN
		CREATE TEMPORARY TABLE IF NOT EXISTS upd_tick_log (
			row_nb INT PRIMARY KEY AUTO_INCREMENT,
            new_tick VARCHAR(50)
		);
        
        CREATE TEMPORARY TABLE IF NOT EXISTS check_fl (
         flag INT DEFAULT 1
        );
        
        IF (SELECT CHAR_LENGTH(flag) FROM check_fl) > 0 THEN
			LEAVE creating_tables;
		ELSE 
			INSERT INTO check_fl
			VALUES (1);
		END IF;
		LEAVE creating_tables;
END//
DELIMITER ;
 
 -- ### 7.2 Tracking updated tickets
 -- Now the update process can go further, and all updated tickets recorded in temprary log
 
DELIMITER //
CREATE TRIGGER storing_updated_tickets
BEFORE UPDATE ON sales
FOR EACH ROW
BEGIN
		SET NEW.total_price_per_unit = NEW.quantity * NEW.unit_price_euro;
        
 		INSERT INTO upd_tick_log (new_tick)
        VALUES (NEW.ticket_number);
END//
DELIMITER ;

-- ### 7.3 Price per ticket update trigger
-- Lastly, the total_price_per_ticket must be updated

DELIMITER //
CREATE TRIGGER update_total_ticket_price
AFTER UPDATE ON sales
FOR EACH ROW
upd:BEGIN
	IF (SELECT flag FROM check_fl) <= (SELECT COUNT(new_tick) FROM upd_tick_log) THEN
	SELECT flag INTO @flag FROM check_fl; 
    
	SET @ticket_to_update = (SELECT new_tick FROM upd_tick_log WHERE row_nb = @flag);
    
	UPDATE total_ticket_purchase
    SET total_price_per_ticket = (SELECT SUM(total_price_per_unit) FROM sales WHERE ticket_number = @ticket_to_update)
    WHERE ticket_number = @ticket_to_update;
    
    UPDATE check_fl SET flag = flag + 1;
    ELSE
		LEAVE upd;
	END IF;
    
	DROP TEMPORARY TABLE upd_tick_log;
	DROP TEMPORARY TABLE check_fl;
END//
DELIMITER ;

-- ##################

-- All columns appear to be normalised
SHOW COLUMNS FROM sales;

-- Lastly, the id column can be discarded
ALTER TABLE sales
DROP COLUMN id;



-- ############### III. Preparing Views ###############
-- ###### 1. Basic information ######
-- ### 1.1 Total income (without refunds)
CREATE VIEW total_income AS
SELECT SUM(total_price_per_unit) AS total_income
FROM sales
WHERE quantity > 0;

-- ### 1.2 Total losses due to refunds
CREATE VIEW total_refunds AS
SELECT SUM(total_price_per_unit)
FROM sales
WHERE quantity < 0;

-- ### 1.3 Clear income (total income + refunds)
CREATE VIEW clear_income AS
SELECT SUM(total_price_per_unit) AS total_income
FROM sales;

-- ### 1.4 Total number of customers as a number of unique ticket numbers
CREATE VIEW total_customers AS
SELECT COUNT(DISTINCT(ticket_number)) AS total_customers
FROM sales
WHERE total_price_per_unit > 0;

-- ###### 2. Total price per ticket ######
-- Collecting values of total purchase for customers who did not do refund
-- 		and did not just get free give-aways.
CREATE VIEW ticket_total_price AS
SELECT ticket_number, total_price_per_ticket
FROM total_ticket_purchase
WHERE total_price_per_ticket > 0;
 
-- ###### 3. Products analysis ######
-- ### 3.1 Top 10 sold products for the entire period
CREATE VIEW top10_total_sales_article AS
SELECT article, 
	SUM(total_price_per_unit) AS total_sales,
    CAST(SUM(total_price_per_unit)*100/(SELECT SUM(total_price_per_unit) FROM sales) AS DECIMAL(6,2)) AS total_sales_pct
FROM sales
GROUP BY article
ORDER BY total_sales DESC
LIMIT 10;

-- ### 3.2 Top 10 sold products in terms of the quantity
CREATE VIEW top10_quant_sal_article AS
SELECT article, 
	SUM(quantity) AS total_quantity,
    CAST(SUM(quantity)*100/(SELECT SUM(quantity) FROM sales) AS DECIMAL(6,2)) AS total_quantity_pct
FROM sales
GROUP BY article
ORDER BY SUM(quantity) DESC
LIMIT 10;


