CREATE TABLE employee_details 
  ( 
     unique_id      NUMBER(10) PRIMARY KEY, 
     dob            DATE, 
     first_name     VARCHAR(20), 
     middle_name    VARCHAR(20), 
     last_name      VARCHAR(20), 
     gender         VARCHAR(6), 
     address_line_1 VARCHAR(20), 
     address_line_2 VARCHAR(20), 
     address_line_3 VARCHAR(20), 
     city           VARCHAR(10), 
     state          VARCHAR(10), 
     country        VARCHAR(10), 
     zip_code       NUMBER(10) 
  ); 

CREATE TABLE person 
  ( 
     person_id     NUMBER(10) PRIMARY KEY, 
     person_number NUMBER(10), 
     dob           DATE, 
     gender        VARCHAR(10) 
  ); 

CREATE TABLE person_name 
  ( 
     name_id     NUMBER(10) PRIMARY KEY, 
     person_id   NUMBER(10), 
     first_name  VARCHAR(10), 
     middle_name VARCHAR(10), 
     last_name   VARCHAR(10) 
  ); 

CREATE TABLE person_address 
  ( 
     address_id     NUMBER(10) PRIMARY KEY, 
     person_id      NUMBER(10), 
     address_line_1 VARCHAR(20), 
     address_line_2 VARCHAR(20), 
     address_line_3 VARCHAR(20), 
     city           VARCHAR(10), 
     state          VARCHAR(10), 
     country        VARCHAR(10), 
     zip_code       NUMBER(10) 
  ); 

CREATE TABLE error_log 
  ( 
     error_id      NUMBER(10), 
     object_name   VARCHAR(1000), 
     unique_id     NUMBER(10), 
     TIME          TIMESTAMP, 
     error_message VARCHAR(1000) 
  ); 

CREATE TABLE client_mapping 
  ( 
     unique_id NUMBER, 
     person_id NUMBER, 
     CONSTRAINT mapping_key PRIMARY KEY(unique_id, person_id) 
  );                             
                          
INSERT INTO employee_details VALUES(1,TO_DATE('21/07/1995', 'DD/MM/YYYY'),'Sabiha',NULL,'V','female','123','2nd Avenue','Anna Nagar','chennai','Tamilnadu','India',668001);
INSERT INTO employee_details VALUES(2,TO_DATE('22/07/1995', 'DD/MM/YYYY'),'Jaggir',NULL,'V','male','113','Indhra street','Second Colony','chennai','Tamilnadu','India',601112);
INSERT INTO employee_details VALUES(3,TO_DATE('24/07/1995', 'DD/MM/YYYY'),'Hafsa',NULL,'K','female','143','Gandhi street','Teachers Quaters','chennai','Tamilnadu','India',608224);
INSERT INTO employee_details VALUES(4,TO_DATE('23/07/1995', 'DD/MM/YYYY'),'Partha',NULL,'G','female',NULL,'1st Main Road','Thiruvallur','chennai',NULL,'India',608449);
INSERT INTO employee_details VALUES(5,TO_DATE('25/07/1995', 'DD/MM/YYYY'),'Moshina',NULL,'V','female','153','Sandwich Street','Surapet','chennai','Tamilnadu','India',608555);
INSERT INTO employee_details VALUES(6,TO_DATE('02/03/1996', 'DD/MM/YYYY'),'Ramya',NULL,'A',NULL,'123',NULL,'Redhills','chennai','Tamilnadu','India',608666);
INSERT INTO employee_details VALUES(7,NULL,'Kavya','M',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO employee_details VALUES(8,TO_DATE('28/07/1995', 'DD/MM/YYYY'),'Gowtham',NULL,'J','male','173','Mckay Street','Tnagar','chennai','Tamilnadu','India',608444);
INSERT INTO employee_details VALUES(9,TO_DATE('20/07/1995', 'DD/MM/YYYY'),'Harini',NULL,'M','female','1409','Adyar','Bridge Road','chennai','Tamilnadu','India',648789);
INSERT INTO employee_details VALUES(10,TO_DATE('20/07/1995', 'DD/MM/YYYY'),'Sai','nila','M','female','148','South Cross street','Pudhur','chennai','Tamilnadu','India',648604);
INSERT INTO employee_details VALUES(11,NULL,'Varsha',NULL,'S','male','1489','2nd Cross Road','Highway Crossing','chennai','Tamilnadu','India',648567);
INSERT INTO employee_details VALUES(12,TO_DATE('20/07/1995', 'DD/MM/YYYY','Jaya',NULL,'M','female',NULL,NULL,NULL,NULL,NULL,NULL,NULL);
 
CREATE SEQUENCE uniq_seq START WITH 1 INCREMENT BY 1;   
CREATE SEQUENCE person_id_seq START WITH 100 INCREMENT BY 1;
CREATE SEQUENCE person_no_seq START WITH 1000 INCREMENT BY 1;  
CREATE SEQUENCE name_id_seq START WITH 100 INCREMENT BY 1; 
CREATE SEQUENCE error_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE address_id_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE TRIGGER person_id_trigger 
BEFORE INSERT ON person
FOR EACH ROW
BEGIN
:NEW.person_id :=person_id_seq.NEXTVAL;
END; 

CREATE OR REPLACE TRIGGER person_no_trigger 
BEFORE INSERT ON person
FOR EACH ROW
BEGIN
:NEW.person_number:=person_no_seq.NEXTVAL;
END; 

CREATE OR REPLACE TRIGGER error_id_trigger 
BEFORE INSERT ON error_log
FOR EACH ROW
BEGIN
:NEW.error_id:=error_id_seq.NEXTVAL;
END; 

CREATE OR REPLACE TRIGGER name_id_trigger 
BEFORE INSERT ON person_name
FOR EACH ROW
BEGIN
:NEW.name_id:=name_id_seq.NEXTVAL;
END; 

CREATE OR REPLACE TRIGGER address_id_trigger 
BEFORE INSERT ON person_address
FOR EACH ROW
BEGIN
:NEW.address_id:=address_id_seq.NEXTVAL;
END; 
