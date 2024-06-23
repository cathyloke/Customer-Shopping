SET SERVEROUTPUT ON SIZE 4000
---insert dataset through sql developer tool before run the pl/sql
---make sure your imported dataset table named "customer_shopping_data"
---your data type for the target table columns :
---invoice_id varchar2(7),
---customer_id varchar2(7),  
---gender varchar2(6),
---age number(3),
---category varchar2(15),
---quantity number(4),
---price number(8,2),
---payment_method varchar2(11),
---invoice_date date,
---shopping_mall varchar2(25),

-------------------------------------------drop table-------------------------------------------
drop table category cascade constraints;
drop table payment cascade constraints;
drop table sales cascade constraints;

------------------------------------------create table------------------------------------------
create table category (
category_id varchar(7), 
category_name varchar2(15), 
constraints category_category_id_pk primary key (category_id)
);

create table payment (
payment_method_id varchar(7), 
payment_method varchar2(11), 
constraints payment_payment_method_id_pk primary key (payment_method_id)
);

create table sales (
invoice_id varchar2(7),
customer_id varchar2(7),  
gender varchar2(6),
age number(3),
category_id varchar2(15),
quantity number(4),
price number(8,2),
payment_method_id varchar2(11),
invoice_date date,
shopping_mall varchar2(25),
constraints sales_invoice_id_pk primary key (invoice_id), 
constraints sales_category_id_fk foreign key (category_id) references category(category_id), 
constraints sales_payment_method_fk foreign key (payment_method_id) references payment(payment_method_id)
);

------------------------------------------import values into 3 tables------------------------------------------
--- 1st table
DECLARE
   loop_counter NUMBER := 1;
BEGIN
   FOR rec IN (SELECT distinct category FROM customer_shopping_data) 
   LOOP
      INSERT INTO category (category_id, category_name)
      VALUES (loop_counter, rec.category);    
      loop_counter := loop_counter + 1;
   END LOOP;
   COMMIT;
END;
/

--- 2nd table
DECLARE
   loop_counter NUMBER := 1;
BEGIN
   FOR rec IN (SELECT distinct payment_method FROM customer_shopping_data) 
   LOOP
      INSERT INTO payment (payment_method_id, payment_method)
      VALUES (loop_counter, rec.payment_method);    
      loop_counter := loop_counter + 1;
   END LOOP;
   COMMIT;
END;
/

--- 3rd table
BEGIN
	INSERT INTO sales (
		invoice_id, 
		customer_id, 
		gender, 
		age, 
		category_id, 
		quantity, 
		price, 
		payment_method_id, 
		invoice_date, 
		shopping_mall)
	SELECT 
		invoice_id, 
		customer_id, 
		gender, 
		age, 
		c.category_id, 
		quantity, 
		price, 
		p.payment_method_id,
		invoice_date, 
		shopping_mall
	FROM customer_shopping_data csd
	join category c on csd.category = c.category_name
	join payment p on csd.payment_method = p.payment_method;
	COMMIT;
END;
/

--- drop original dataset
DROP TABLE customer_shopping_data CASCADE CONSTRAINTS; 

------------------------------------------INDEXES------------------------------------------
CREATE INDEX idx_sales_category ON sales(category_id); 
CREATE INDEX idx_sales_payment_method ON sales(payment_method_id); 
CREATE INDEX idx_sales_invoice_date ON sales(invoice_date); 


------------------------------------------CRUD operation------------------------------------------
--------sales table--------
--- Create record
CREATE OR REPLACE PROCEDURE insert_record_sales(
	p_invoice_id IN VARCHAR2,
	p_customer_id IN VARCHAR2,
	p_gender IN VARCHAR2,
	p_age IN NUMBER, 
	p_category_id IN VARCHAR2, 
	p_quantity IN NUMBER, 
	p_price IN NUMBER, 
	p_payment_method_id IN VARCHAR2, 
	p_invoice_date IN DATE, 
	p_shopping_mall IN VARCHAR2) 
IS
	category_count NUMBER;
    invoice_count NUMBER;
BEGIN 
	SELECT COUNT(*) INTO category_count
    FROM category
    WHERE category_id = p_category_id;
    IF category_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Category ID does not exist');
    END IF;
	
	SELECT COUNT(*) INTO invoice_count
    FROM sales
    WHERE invoice_id = p_invoice_id;
    IF invoice_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Invoice ID already exists');
    END IF;
		
	INSERT INTO sales 
	VALUES (p_invoice_id, p_customer_id, p_gender, p_age, p_category_id, 
        p_quantity, p_price, p_payment_method_id, p_invoice_date, p_shopping_mall);
	COMMIT; 	
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--- Retrieve record from sales
CREATE OR REPLACE FUNCTION retrieve_record_sales(columnName VARCHAR2, operator VARCHAR2, searchCondition VARCHAR2)
RETURN SYS_REFCURSOR
IS 
	c SYS_REFCURSOR;
	sql_query VARCHAR2(300);
	result_row sales%ROWTYPE;
BEGIN 
	sql_query := 'SELECT * FROM sales WHERE ' || columnName || operator || ' :x';
	OPEN c FOR sql_query USING searchCondition;
	LOOP
        FETCH c INTO result_row;
        EXIT WHEN c%NOTFOUND;
        
		DBMS_OUTPUT.PUT_LINE(
            RPAD(result_row.invoice_id, 8) || 
            RPAD(result_row.customer_id, 9) || 
            RPAD(result_row.gender, 7) || 
            RPAD(result_row.age, 4) || 
            RPAD(result_row.category_id, 9) || 
            RPAD(result_row.quantity, 9) || 
            RPAD(result_row.price, 10) || 
            RPAD(result_row.payment_method_id, 15) || 
            RPAD(TO_CHAR(result_row.invoice_date, 'DD-MON-YYYY'), 13) || 
            RPAD(result_row.shopping_mall, 25)
        );
    END LOOP;
	RETURN c;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--- Update record
CREATE OR REPLACE PROCEDURE update_record_sales(
	p_primarykey IN VARCHAR2, 
	p_columnName IN VARCHAR2, 
	p_newValue IN VARCHAR2) 
IS 
	v_sql VARCHAR2(1000); 
	invoice_count NUMBER;
BEGIN 
	SELECT COUNT(*) INTO invoice_count
    FROM sales
    WHERE invoice_id = p_primarykey;

    IF invoice_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Invoice ID does not exists');
    END IF;
	
	v_sql := 'UPDATE sales SET ' || p_columnName || ' = :1 WHERE invoice_id = :2'; 
    EXECUTE IMMEDIATE v_sql USING p_newvalue, p_primarykey;
    COMMIT;
	
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--- Delete record
CREATE OR REPLACE PROCEDURE delete_record_sales(p_primarykey IN VARCHAR2)
IS 
	invoice_count NUMBER;
	v_sql VARCHAR2(1000);
BEGIN 
	SELECT COUNT(*) INTO invoice_count
    FROM sales
    WHERE invoice_id = p_primarykey;

    IF invoice_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Invoice ID does not exists');
    END IF;
	
	v_sql := 'DELETE FROM sales WHERE invoice_id = :x';
	EXECUTE IMMEDIATE v_sql USING p_primarykey;
	
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--------payment table--------
--- Create record
CREATE OR REPLACE PROCEDURE insert_record_payment(
	p_payment_method_id IN VARCHAR2,
	p_payment_method IN VARCHAR2) 
IS
	payment_method_count NUMBER;
BEGIN 
	SELECT COUNT(*) INTO payment_method_count
    FROM payment
    WHERE payment_method_id = p_payment_method_id;

    IF payment_method_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Payment method id exists');
    END IF;
	
	INSERT INTO payment 
	VALUES (p_payment_method_id, p_payment_method);
	COMMIT; 
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--------category table--------
--- Create record
CREATE OR REPLACE PROCEDURE insert_record_category(
	p_category_id IN VARCHAR2,
	p_category_name IN VARCHAR2) 
IS
	category_count NUMBER;
BEGIN 
	SELECT COUNT(*) INTO category_count
    FROM category
    WHERE category_id = p_category_id;

    IF category_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Category id exists');
    END IF;
	
	INSERT INTO category 
	VALUES (p_category_id, p_category_name);
	COMMIT; 
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/


-------------------------------------------TRIGGERS-------------------------------------------
--- Before Insert Trigger
CREATE OR REPLACE TRIGGER before_insert_trigger
BEFORE INSERT ON sales
FOR EACH ROW
DECLARE
	category_count NUMBER;
	invoice_count NUMBER;
BEGIN
    IF :NEW.gender != 'Male' AND :NEW.gender != 'Female' THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Gender must be either Male or Female');
    ELSIF :NEW.age < 0 OR :NEW.age > 100 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Age must be in the range of 0 - 100');
    ELSIF :NEW.quantity <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Quantity must be greater than 0');
    ELSIF :NEW.price <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Price must be greater than 0');
    ELSIF NOT REGEXP_LIKE(:NEW.invoice_date, '^\d{2}-[A-Z]{3}-\d{2}$') THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid data: Invoice date must be in DD-MON-RR format');
    END IF;
END;
/

---create new payment method
CREATE OR REPLACE TRIGGER before_insert_payment_trigger
BEFORE INSERT ON payment
FOR EACH ROW
DECLARE
    payment_method_id_count NUMBER;
    payment_method_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO payment_method_id_count
    FROM payment
    WHERE payment_method_id = :NEW.payment_method_id;

    SELECT COUNT(*) INTO payment_method_count
    FROM payment
    WHERE payment_method = :NEW.payment_method;

    IF payment_method_id_count > 0 OR payment_method_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Insert fail. Payment method exists.');
    END IF;
END;
/

---create new category
CREATE OR REPLACE TRIGGER before_insert_category_trigger
BEFORE INSERT ON category
FOR EACH ROW
DECLARE
	category_id_count NUMBER;
	category_name_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO category_id_count
    FROM category
    WHERE category_id = :NEW.category_id;
	
	IF category_id_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Insert fail. Category ID exist.');
    END IF;
	
	SELECT COUNT(*) INTO category_name_count
    FROM category
    WHERE category_name = :NEW.category_name;

	IF category_name_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Insert fail. Category name exist.');
    END IF;
END;
/

--- After Insert Trigger
CREATE OR REPLACE TRIGGER after_insert_trigger
AFTER INSERT ON sales
FOR EACH ROW
DECLARE
    v_output_message VARCHAR2(1000);
BEGIN
    v_output_message := 'New Record Inserted:' || CHR(10) ||
                        'Invoice No: ' || :NEW.invoice_id || CHR(10) ||
                        'Customer ID: ' || :NEW.customer_id || CHR(10) ||
                        'Gender: ' || :NEW.gender || CHR(10) ||
                        'Age: ' || :NEW.age || CHR(10) ||
                        'Category ID: ' || :NEW.category_id || CHR(10) ||
                        'Quantity: ' || :NEW.quantity || CHR(10) ||
                        'Price: ' || :NEW.price || CHR(10) ||
                        'Payment Method ID: ' || :NEW.payment_method_id || CHR(10) ||
                        'Invoice Date: ' || TO_CHAR(:NEW.invoice_date, 'DD-MON-YYYY') || CHR(10) ||
                        'Shopping Mall: ' || :NEW.shopping_mall;

    DBMS_OUTPUT.PUT_LINE(v_output_message);
	DBMS_OUTPUT.PUT_LINE('Record inserted successfully.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--- payment
CREATE OR REPLACE TRIGGER after_insert_payment_trigger
AFTER INSERT ON payment
FOR EACH ROW
DECLARE
    v_output_message VARCHAR2(1000);
BEGIN
    v_output_message := 'New Record Inserted:' || CHR(10) ||
                        'Payment Method ID: ' || :NEW.payment_method_id || CHR(10) ||
                        'Payment Method: ' || :NEW.payment_method;

    DBMS_OUTPUT.PUT_LINE(v_output_message);
	DBMS_OUTPUT.PUT_LINE('Record inserted successfully.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

---category
CREATE OR REPLACE TRIGGER after_insert_category_trigger
AFTER INSERT ON category
FOR EACH ROW
DECLARE
    v_output_message VARCHAR2(1000);
BEGIN
    v_output_message := 'New Record Inserted:' || CHR(10) ||
                        'Category ID: ' || :NEW.category_id || CHR(10) ||
                        'Category Name: ' || :NEW.category_name;

    DBMS_OUTPUT.PUT_LINE(v_output_message);
	DBMS_OUTPUT.PUT_LINE('Record inserted successfully.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--- Before Update Trigger
CREATE OR REPLACE TRIGGER before_update_trigger
BEFORE UPDATE ON sales
FOR EACH ROW
BEGIN
    IF :NEW.gender != 'Male' AND :NEW.gender != 'Female' THEN
        RAISE_APPLICATION_ERROR(-20002, 'Invalid data: Gender must be either Male or Female');
    ELSIF :NEW.age < 0 OR :NEW.age > 100 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Invalid data: Age must be in the range of 0 - 100');
    ELSIF :NEW.quantity <= 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Invalid data: Quantity must be greater than 0');
    ELSIF :NEW.price <= 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Invalid data: Price must be greater than 0');
    ELSIF NOT REGEXP_LIKE(:NEW.invoice_date, '^\d{2}-[A-Z]{3}-\d{2}$') THEN
        RAISE_APPLICATION_ERROR(-20002, 'Invalid data: Invoice date must be in DD-MON-RR format');
    END IF;
END;
/

--- After Update Trigger
CREATE OR REPLACE TRIGGER after_update_trigger
AFTER UPDATE ON sales
FOR EACH ROW
DECLARE
    v_output_message VARCHAR2(2000);
BEGIN
    v_output_message := 'Record Updated:' || CHR(10) ||
                        'Invoice No: ' || :NEW.invoice_id || CHR(10) ||
                        'Customer ID: ' || :NEW.customer_id || CHR(10) ||
                        'Gender: ' || :NEW.gender || CHR(10) ||
                        'Age: ' || :NEW.age || CHR(10) ||
                        'Category ID: ' || :NEW.category_id || CHR(10) ||
                        'Quantity: ' || :NEW.quantity || CHR(10) ||
                        'Price: ' || :NEW.price || CHR(10) ||
                        'Payment Method ID: ' || :NEW.payment_method_id || CHR(10) ||
                        'Invoice Date: ' || TO_CHAR(:NEW.invoice_date, 'DD-MON-YYYY') || CHR(10) ||
                        'Shopping Mall: ' || :NEW.shopping_mall || CHR(10) ||
                        '-------------------------------------------' || CHR(10) ||
                        'Previous Values:' || CHR(10) ||
                        'Invoice No: ' || :OLD.invoice_id || CHR(10) ||
                        'Customer ID: ' || :OLD.customer_id || CHR(10) ||
                        'Gender: ' || :OLD.gender || CHR(10) ||
                        'Age: ' || :OLD.age || CHR(10) ||
                        'Category ID: ' || :OLD.category_id || CHR(10) ||
                        'Quantity: ' || :OLD.quantity || CHR(10) ||
                        'Price: ' || :OLD.price || CHR(10) ||
                        'Payment Method ID: ' || :OLD.payment_method_id || CHR(10) ||
                        'Invoice Date: ' || TO_CHAR(:OLD.invoice_date, 'DD-MON-YYYY') || CHR(10) ||
                        'Shopping Mall: ' || :OLD.shopping_mall || CHR(10);

    DBMS_OUTPUT.PUT_LINE(v_output_message);
	DBMS_OUTPUT.PUT_LINE('Record updated successfully.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

--- Before Delete Trigger
CREATE OR REPLACE TRIGGER before_delete_trigger
BEFORE DELETE ON sales
FOR EACH ROW
DECLARE
    category_count NUMBER;
	payment_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO category_count
    FROM category
    WHERE category_id = :OLD.category_id;

    IF category_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Cannot delete: Category does not exist');
    END IF;
	
	SELECT COUNT(*) INTO payment_count
    FROM payment
    WHERE payment_method_id = :OLD.payment_method_id;

    IF payment_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Cannot delete: Payment method does not exist');
    END IF;
END;
/

--- After Delete Trigger
CREATE OR REPLACE TRIGGER after_delete_trigger
AFTER DELETE ON sales
FOR EACH ROW
DECLARE
    v_output_message VARCHAR2(100);
BEGIN
    v_output_message := 'Record Deleted:' || CHR(10) ||
                        'Invoice No: ' || :OLD.invoice_id;

    DBMS_OUTPUT.PUT_LINE(v_output_message);
	DBMS_OUTPUT.PUT_LINE('Record deleted successfully.');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/


------------------------------TEST ON CRUD OPERATION---------------------------------------------------- 
---create a new record
---create a new sales
DECLARE
    v_invoice_id VARCHAR2(7);
    v_customer_id VARCHAR2(7);
    v_gender VARCHAR2(6);
    v_age NUMBER(3);
    v_category_id VARCHAR2(15);
    v_quantity NUMBER(4);
    v_price NUMBER(8, 2);
    v_payment_method_id VARCHAR2(11);
    v_invoice_date DATE;
    v_shopping_mall VARCHAR2(25);
BEGIN
	SAVEPOINT sv_sales_insert;

    v_invoice_id := '&New_Invoice_id';
    v_customer_id := '&Customer_ID';
    v_gender := '&Gender';
    v_age := &Age;
    v_category_id := '&Category_ID';
    v_quantity := &Quantity;
    v_price := &Price;
    v_payment_method_id := '&Payment_Method_ID';
    v_invoice_date := TO_DATE('&Invoice_Date', 'DD-MON-YYYY');
    v_shopping_mall := '&Shopping_Mall';
   
    insert_record_sales(
        v_invoice_id, 
        v_customer_id, 
        v_gender, 
        v_age, 
        v_category_id, 
        v_quantity, 
        v_price,  
        v_payment_method_id,
        v_invoice_date, 
        v_shopping_mall);
    
EXCEPTION
    WHEN OTHERS THEN ROLLBACK TO sv_sales_insert;
        DBMS_OUTPUT.PUT_LINE('Rolling back. Error: ' || SQLERRM);
END;
/

---create a new payment method
DECLARE
    v_payment_method_id VARCHAR2(7);
    v_payment_method VARCHAR2(11);
BEGIN
	SAVEPOINT sv_payment_insert;

    v_payment_method_id := '&New_Payment_Method_ID';
    v_payment_method := '&New_Payment_Method';
   
    insert_record_payment(
        v_payment_method_id, 
		v_payment_method);
EXCEPTION
    WHEN OTHERS THEN ROLLBACK TO sv_payment_insert;
        DBMS_OUTPUT.PUT_LINE('Rolling back. Error: ' || SQLERRM);
END;
/

---create a new category
DECLARE
    v_category_id VARCHAR2(7);
    v_category_name VARCHAR2(15);
BEGIN
	SAVEPOINT sv_category_insert;

    v_category_id := '&New_Category_ID';
    v_category_name := '&New_Category_Name';
   
    insert_record_category(
        v_category_id, 
		v_category_name);
EXCEPTION
    WHEN OTHERS THEN ROLLBACK TO sv_category_insert;
        DBMS_OUTPUT.PUT_LINE('Rolling back. Error: ' || SQLERRM);
END;
/

---retrieve record
DECLARE
    v_column_name VARCHAR2(30);
    v_operator VARCHAR2(2);
    v_search_condition VARCHAR2(100);
    
    v_invoice_id sales.invoice_id%TYPE;
    v_customer_id sales.customer_id%TYPE;
    v_gender sales.gender%TYPE;
    v_age sales.age%TYPE;
    v_category_id sales.category_id%TYPE;
    v_quantity sales.quantity%TYPE;
    v_price sales.price%TYPE;
    v_payment_method_id sales.payment_method_id%TYPE;
    v_invoice_date sales.invoice_date%TYPE;
    v_shopping_mall sales.shopping_mall%TYPE;
    v_output_message VARCHAR2(1000);

    c_results SYS_REFCURSOR;
BEGIN
    v_column_name := '&Column_Name';
    v_operator := '&Operator';
    v_search_condition := '&Search_Condition';
    
    DBMS_OUTPUT.PUT_LINE(
        RPAD('Invoice ID', 8) || 
        RPAD('Customer ID', 9) || 
        RPAD('Gender', 7) || 
        RPAD('Age', 4) || 
        RPAD('Category ID', 9) || 
        RPAD('Quantity', 9) || 
        RPAD('Price', 10) || 
        RPAD('Payment Method ID', 15) || 
        RPAD('Invoice Date', 13) || 
        RPAD('Shopping Mall', 25)
    );
    c_results := retrieve_record_sales(v_column_name, v_operator, v_search_condition);
    FETCH c_results INTO 
		v_invoice_id, 
		v_customer_id, 
		v_gender, 
		v_age, 
		v_category_id,
        v_quantity, 
		v_price, 
		v_payment_method_id, 
		v_invoice_date, 
		v_shopping_mall;
    CLOSE c_results;

    DBMS_OUTPUT.PUT_LINE(
            RPAD(v_invoice_id, 8) || 
            RPAD(v_customer_id, 9) || 
            RPAD(v_gender, 7) || 
            LPAD(v_age, 4) || 
            LPAD(v_category_id, 9) || 
            LPAD(v_quantity, 9) || 
            LPAD(v_price, 10) || 
            RPAD(v_payment_method_id, 15) || 
            RPAD(TO_CHAR(v_invoice_date, 'DD-MON-YYYY'), 13) || 
            RPAD(v_shopping_mall, 25)
        );

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No records found.');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/

---update record
DECLARE
    v_primary_key_value sales.invoice_id%TYPE;
    v_column_name VARCHAR2(30);
    v_new_value VARCHAR2(100);
BEGIN
	SAVEPOINT sv_update_sales;

    v_primary_key_value := '&Invoice_Id_To_Update';
    v_column_name := '&Column_Name_To_Update';
    v_new_value := '&New_Value';

    update_record_sales(v_primary_key_value, v_column_name, v_new_value);   
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No records found.');
    WHEN OTHERS THEN ROLLBACK TO sv_update_sales;
        DBMS_OUTPUT.PUT_LINE('Rolling back. Error: ' || SQLERRM);
END;
/

---delete record
DECLARE
    v_primary_key_value sales.invoice_id%TYPE;
BEGIN
	SAVEPOINT sv_delete_sales;

    v_primary_key_value := '&Invoice_Id_To_Delete';
	
    delete_record_sales(v_primary_key_value);
    
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No records found.');
    WHEN OTHERS THEN ROLLBACK TO sv_delete_sales;
        DBMS_OUTPUT.PUT_LINE('Rolling back. Error: ' || SQLERRM);
END;
/

----------------------------------------QUERY-----------------------------------------------------
-- a function to convert age as integer to age interval
-- eg. age 25 becomes interval 20 - 29 
CREATE OR REPLACE FUNCTION agegroup(age VARCHAR2) 
RETURN VARCHAR2
IS 
    result_ VARCHAR2(10);
    a1 VARCHAR2(1); 
BEGIN 
    a1 := SUBSTR(age, 1, 1); 
    result_ := a1 || '0 - ' || a1 || '9';
    RETURN result_;
END;
/

-- QUERY 1
-- To find the frequency of the payment method used by each age group
DECLARE
	CURSOR payment_method_cursor IS
		SELECT agegroup(s.age) AS age_group, p.payment_method, COUNT(s.payment_method_id) AS frequency 
		FROM sales s
		JOIN payment p on s.payment_method_id = p.payment_method_id
		GROUP BY agegroup(s.age), p.payment_method 
		ORDER BY agegroup(s.age), COUNT(s.payment_method_id) DESC;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Age Group', 10) || RPAD('Payment Method', 15) || LPAD('Frequency', 10));
	FOR row IN payment_method_cursor
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.age_group, 10) || RPAD(row.payment_method, 15) || LPAD(row.frequency, 10));
		END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 2 
-- gender, age group, frequency 
DECLARE
	CURSOR agegroup_cursor IS
		SELECT gender, agegroup(age) AS age_group, COUNT(*) AS frequency 
		FROM sales 
		GROUP BY gender, agegroup(age)
		ORDER BY gender, agegroup(age);
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Gender', 10) || RPAD('Age Group', 10) || LPAD('Frequency', 10));
	FOR row IN agegroup_cursor 
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.gender, 10) || RPAD(row.age_group, 10) || LPAD(row.frequency, 10));
		END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 3
-- To find the frequency of customers and the average age for each gender
DECLARE
	CURSOR age_average_cursor IS
		SELECT gender, AVG(age) AS average_age, COUNT(*) AS frequency
		FROM sales
		GROUP BY gender;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Gender', 10) || LPAD('Average age', 15) || LPAD('Frequency', 15));
	FOR row IN age_average_cursor 
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.gender, 10) || LPAD(TO_CHAR(row.average_age, '99999.99'), 15) || LPAD(row.frequency, 15));
		END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 4
-- Calculate average quantity sold by age group 
DECLARE
    CURSOR age_group_cursor IS
        SELECT agegroup(age) AS age_group, ROUND(AVG(quantity)) AS avg_quantity
        FROM sales
        GROUP BY agegroup(age);
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Age Group', 10) || LPAD('Average Quantity Sold', 25));
	FOR row IN age_group_cursor 
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.age_group, 10) || LPAD(row.avg_quantity, 25));
		END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/
 
-- QUERY 5
-- To find the frequency of customers for each gender and each category
DECLARE
	CURSOR category_cursor IS
		SELECT s.gender, c.category_name, SUM(s.quantity) AS frequency 
		FROM sales s, category c
		where s.category_id = c.category_id
		GROUP BY s.gender, c.category_name
		ORDER BY s.gender, SUM(s.quantity) DESC;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Gender', 10) || RPAD('Category', 18) || LPAD('Frequency', 10));
	FOR row IN category_cursor
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.gender, 10) || RPAD(row.category_name, 18) || LPAD(row.frequency, 10));
        END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 6
-- To find the annual revenue for each shopping mall
DECLARE
    CURSOR sales_cursor IS
        SELECT TO_CHAR(invoice_date, 'YYYY') AS year, shopping_mall, SUM(price) AS revenue
        FROM sales
        GROUP BY TO_CHAR(invoice_date, 'YYYY'), shopping_mall
        ORDER BY TO_CHAR(invoice_date, 'YYYY'), SUM(price) DESC;
    
    v_year VARCHAR2(4);
    v_shopping_mall VARCHAR2(25);
    v_revenue NUMBER(15, 2); -- Adjust the precision as needed
BEGIN
    OPEN sales_cursor;
	DBMS_OUTPUT.PUT_LINE(RPAD('Year', 6) || RPAD('Shopping Mall', 32) || RPAD('Revenue (TL)', 12));
    LOOP
        FETCH sales_cursor INTO v_year, v_shopping_mall, v_revenue;
        EXIT WHEN sales_cursor%NOTFOUND; 
        DBMS_OUTPUT.PUT_LINE(RPAD(v_year, 6) || RPAD(v_shopping_mall, 25) || TO_CHAR(v_revenue, '999,999,999,999.99') );
    END LOOP;
    CLOSE sales_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 7
-- Calculate total revenue for each shopping mall
DECLARE
    CURSOR mall_revenue_cursor IS
        SELECT shopping_mall, SUM(quantity * price) AS total_revenue
        FROM sales
        GROUP BY shopping_mall;
BEGIN
    DBMS_OUTPUT.PUT_LINE(RPAD('Shopping Mall', 25) || LPAD('Total Revenue', 20));
	FOR row IN mall_revenue_cursor 
	LOOP    
		DBMS_OUTPUT.PUT_LINE(RPAD(row.shopping_mall, 25) || LPAD(TO_CHAR(row.total_revenue, 'FM999G999G999D00') || ' TL', 20));
	END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 8
-- To find total number of sales for each shopping mall
DECLARE
	CURSOR sales_cursor IS
		SELECT shopping_mall, COUNT(*) AS num_sales
		FROM sales
		GROUP BY shopping_mall
		ORDER BY num_sales ASC;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Shopping Mall', 25) || LPAD('Sales', 10));
	FOR row IN sales_cursor
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.shopping_mall, 25) || LPAD(row.num_sales, 10));
        END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 9
-- To find the loyalty customer
DECLARE
	CURSOR purchase_cursor IS
		SELECT customer_id, COUNT(DISTINCT invoice_id) AS num_purchases
		FROM sales
		GROUP BY customer_id
		HAVING COUNT(DISTINCT invoice_id) > 1;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Customer ID', 15) || LPAD('Purchases', 10));
	FOR row IN purchase_cursor
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.customer_id, 15) || LPAD(row.num_purchases, 10));
        END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 10
-- To find the popular payment methods used by customers throughout the 3 years
DECLARE
	CURSOR customer_cursor IS
		SELECT p.payment_method as method, COUNT(*) AS no_customers
		FROM sales s
		JOIN payment p on s.payment_method_id = p.payment_method_id
		GROUP BY p.payment_method
		ORDER BY no_customers DESC;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Payment Method', 20) || LPAD('Frequency', 10));
	FOR row IN customer_cursor
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.method, 20) || LPAD(row.no_customers, 10));
        END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/
		
-- QUERY 11
-- To find the category which having highest sales
DECLARE
	CURSOR customer_cursor IS
		SELECT c.category_name, AVG(s.price * s.quantity) AS avg_price, AVG(s.quantity) AS avg_quantity
		FROM sales s
		JOIN category c ON s.category_id = c.category_id
		GROUP BY c.category_name;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Category', 16) || LPAD('AVG Price', 18) || LPAD('AVG Quantity', 15));
	FOR row IN customer_cursor
		LOOP    
			DBMS_OUTPUT.PUT_LINE(RPAD(row.category_name, 16) || LPAD(TO_CHAR(row.avg_price, '999999.99'), 15) || ' TL' ||LPAD(TO_CHAR(row.avg_quantity, '999999.99'), 15));
        END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/

-- QUERY 12
-- Get total sales amount for each category
DECLARE
    CURSOR sales_cursor IS
        SELECT c.category_name, SUM(s.quantity * s.price) AS total_sales_amount
        FROM sales s
        JOIN category c ON s.category_id = c.category_id
        GROUP BY c.category_name;
BEGIN
	DBMS_OUTPUT.PUT_LINE(RPAD('Category', 20) || RPAD(' Total Sales Amount', 20));
    FOR rec IN sales_cursor LOOP
        DECLARE
            v_formatted_amount VARCHAR2(100);
        BEGIN
            -- Convert the total sales amount to Turkish Liras (TL) format
            DBMS_OUTPUT.PUT_LINE(RPAD(rec.category_name, 20) || LPAD(TO_CHAR(rec.total_sales_amount, 'FM999G999G999D00'), 16) || ' TL');
        END;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END;
/





