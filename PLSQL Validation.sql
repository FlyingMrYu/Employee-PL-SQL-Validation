--
--                   +===================================================================+
--                   |              Employee_Data_Seperation_and_Validation              |
--                   +===================================================================+
--    
CREATE OR REPLACE PACKAGE BODY data_simulation 
    IS
-----------------------------------------------------------------------------------------------------------------------  
--                                          global variable declaration
----------------------------------------------------------------------------------------------------------------------- 
       gt_timestamp TIMESTAMP:=SYSTIMESTAMP;
       gv_error_msg VARCHAR(500):=NULL;
       gv_object_name VARCHAR(500):=NULL;
       gv_status NUMBER:=0;
-----------------------------------------------------------------------------------------------------------------------  
--                                             procedure declaration
----------------------------------------------------------------------------------------------------------------------- 
       PROCEDURE employee_details;
       PROCEDURE person_procedure(p_dob_i               IN DATE
                                 ,p_gender_i            IN VARCHAR
                                 ,p_person_id_i         IN NUMBER
                                 );
       PROCEDURE integration_map(p_uniq_id_i            IN NUMBER
                                ,p_person_id_o         OUT NUMBER
                                );
       PROCEDURE name_procedure(p_first_name_i          IN VARCHAR
                               ,p_middle_name_i         IN VARCHAR
                               ,p_last_name_i           IN VARCHAR
                               ,p_person_id_i           IN NUMBER
                               ,p_u_person_id_i         IN NUMBER
                               );
       PROCEDURE address_procedure(p_address_line_1_i   IN VARCHAR
                                  ,p_address_line_2_i   IN VARCHAR
                                  ,p_address_line_3_i   IN VARCHAR
                                  ,p_city_i             IN VARCHAR
                                  ,p_state_i            IN VARCHAR
                                  ,p_country_i          IN VARCHAR
                                  ,p_zip_code_i         IN VARCHAR
                                  ,p_person_id_i        IN NUMBER
                                  ,p_u_person_id_i      IN NUMBER
                                  ,p_address_id_i       IN NUMBER
                                  );
       PROCEDURE error_log(p_obj_name_i                 IN VARCHAR
                          ,p_err_msg_i                  IN VARCHAR
                          ,p_uniq_id_i                  IN NUMBER
                          );                        
-----------------------------------------------------------------------------------------------------------------------  
--                                                 main procedure definition
----------------------------------------------------------------------------------------------------------------------- 
       --this function calls the employee_details procedure 
       PROCEDURE main
              IS
       BEGIN
       --
          employee_details();
       --
       EXCEPTION
       --
          WHEN OTHERS
          --
          THEN
          --
             dbms_output.put_line(sqlerrm);
          --   
       --      
       END;
-----------------------------------------------------------------------------------------------------------------------
--                                                  employee_details definition
-----------------------------------------------------------------------------------------------------------------------
       --This procedure checks whether client record is present or not,if present it calls the subsequent procedures
       PROCEDURE employee_details
       --
              IS
              --
              --The following cursor fetches the client details. If the client details are already present in the database it will return the respected client details to update; else it will return null
              --
          CURSOR client_cursor
          --
              IS 
              --
          SELECT cr.unique_id          
                ,cr.dob
                ,cr.first_name          
                ,cr.middle_name         
                ,cr.last_name           
                ,cr.gender
                ,cr.address_line_1      
                ,cr.address_line_2     
                ,cr.address_line_3      
                ,cr.city                
                ,cr.state               
                ,cr.country             
                ,cr.zip_code      
                ,cm.person_id --return person_id if the record is already present; else it will return null      
                ,(SELECT pa.address_id
                    FROM person_address pa
                   WHERE pa.person_id(+)=cm.person_id
                         )  address_id        
            FROM employee_details   cr
                ,client_mapping  cm
           WHERE cr.unique_id=cm.unique_id(+);
            --
            lv_record_check VARCHAR(3):='N';
            lv_person_status VARCHAR(3);
            lv_person_flag VARCHAR(3);
            ln_row_count NUMBER:=0;
            ln_person_id NUMBER;
            --
       BEGIN
       --
          FOR v_employee_details IN client_cursor
          --
             LOOP
             --
                BEGIN
                --
                   lv_record_check:='Y';
                   SAVEPOINT S1;
                      --
                      --calling person_procedure
                      --
                      person_procedure(v_employee_details.dob
                                      ,v_employee_details.gender
                                      ,v_employee_details.person_id
                                     );
                      --
                      --calling integration map procedure
                      --
                      integration_map(v_employee_details.unique_id
                                     ,ln_person_id
                                      );
                      --
                      --calling name procedure 
                      --
                      name_procedure(v_employee_details.first_name
                                    ,v_employee_details.middle_name
                                    ,v_employee_details.last_name
                                    ,ln_person_id
                                    ,v_employee_details.person_id
                                    );
                      --
                      --address will be checked only if any of the address record is present; if not the address will not be validated
                      --
                      IF(v_employee_details.address_line_1 IS NOT NULL) OR (v_employee_details.address_line_2 IS NOT NULL)OR(v_employee_details.address_line_3 IS NOT NULL) OR (v_employee_details.city IS NOT NULL) OR (v_employee_details.state IS NOT NULL) OR (v_employee_details.country IS NOT NULL)OR(v_employee_details.zip_code IS NOT NULL)
                      --                      
                      THEN
                      --
                      --calling address procedure    
                      --                      
                      address_procedure(v_employee_details.address_line_1
                                       ,v_employee_details.address_line_2
                                       ,v_employee_details.address_line_3
                                       ,v_employee_details.city
                                       ,v_employee_details.state
                                       ,v_employee_details.country
                                       ,v_employee_details.zip_code
                                       ,ln_person_id
                                       ,v_employee_details.person_id
                                       ,v_employee_details.address_id
                                      );
                      --
                      END IF;
                      --
                      --if there is any error error_msg will be stored in the error_log table
                      --
                      IF(gv_error_msg IS NOT NULL)
                      --
                      THEN
                      --
                      --calling error_log procedure to store error details in error_log table
                      --
                         error_log(LTRIM(gv_object_name,',')
                                  ,LTRIM(gv_error_msg,',')
                                  ,v_employee_details.unique_id
                                  );
                      --
                      --rollback to the save-point to undo the changes when error occurs
                      --
                         ROLLBACK TO s1;
                      --
                      --re-initializing gv_error_msg and gv_object_name to NULL
                      --
                      gv_error_msg:=NULL;
                      gv_object_name:=NULL;
                      gv_status:=0;
                      --
                      END IF;
                      --
                      --checking count of inserted record if it is 10 then those records will be committed
                      --
                        ln_row_count:=ln_row_count+1;
                      IF((MOD(ln_row_count,10))=0)
                      --
                      THEN
                      --
                      --committing remaining records
                      --
                         COMMIT;
                        --
                      END IF;
                      --
                EXCEPTION
                --
                    WHEN OTHERS
                    --
                    THEN
                    --
                       dbms_output.put_line(sqlerrm);
                       ROLLBACK;
                    --
                 END;
                 --
             END LOOP;
             --
             --checking whether record is null
             --
             IF(lv_record_check='N')
             --
             THEN
             --
                dbms_output.put_line('no client record found');
                --
             END IF;
             --
             COMMIT;
             --
       EXCEPTION
       --
          WHEN OTHERS 
          --
          THEN
          --
            dbms_output.put_line(sqlerrm);
            --
            ROLLBACK;
            --
       END;
-----------------------------------------------------------------------------------------------------------------------
--                                                  person_procedure definition
-----------------------------------------------------------------------------------------------------------------------
       --this procedure validates the DOB,gender if present it will store the record in person table
       PROCEDURE person_procedure(p_dob_i           IN DATE
                                 ,p_gender_i        IN VARCHAR
                                 ,p_person_id_i     IN NUMBER
                                )
              IS
              --
       BEGIN
       --
       --checking whether DOB is null or not
       --
          IF(p_dob_i IS NULL)
          --
          THEN
          --
             gv_error_msg:='dob is mandatory';
             --
          END IF;
          --
          IF(p_gender_i IS NULL)
          --
          --checking whether gender is null or not
          --
          THEN
          --
             gv_error_msg:=gv_error_msg||','||'gender is mandatory';
             --
          END IF;
          --
          --checking whether error_msg is null or not,if null data will be stored in person
          --
          IF(gv_error_msg IS NULL)
          --
          THEN
          -- 
             --
             --checking whether the person_id(client data) is already present or not
             --
             IF(p_person_id_i IS NULL)
             --
             THEN
             --
                INSERT INTO person(dob
                                         ,gender
                                         )
                                   VALUES(p_dob_i
                                         ,p_gender_i
                                         );
             --
             --setting status to 1 to know the data is inserted               
             gv_status:=1;
             ELSE
               --
               --updating the data if it is already stored in the database
               --
               UPDATE person
                  SET dob=p_dob_i
                     ,gender=p_gender_i
                WHERE person_id=p_person_id_i;
                --
                --setting status to 2 to know the data is updated 
                --
               gv_status:=2;
             --
             END IF;
          ELSE
             --
             --storing object name to know in which table the data got errored
             --
             gv_object_name:='person';
             --
          END IF;
            --
       EXCEPTION 
       --
          WHEN OTHERS
          --
          THEN
          --
             dbms_output.put_line(sqlerrm);
             ROLLBACK;
             --
       END;
-----------------------------------------------------------------------------------------------------------------------
--                                             integration_map definition
-----------------------------------------------------------------------------------------------------------------------
       --
       --this procedure stores the unique_id(client given) and person_id(system generated) for mapping
       --
       PROCEDURE integration_map(p_uniq_id_i       IN NUMBER
                                ,p_person_id_o    OUT NUMBER
                                )
              IS
              --
              --this cursor stores the max person_id from the person table
              --
          CURSOR pid_cursor
              --          
              IS
              --
          SELECT MAX(person_id) p_id
            FROM person;
            --
        BEGIN
        -- 
           --
           --checking whether the data is inserted or updated in the previous procedures,inserts the unique_id and person_id only if the data is inseted not for updation
           --
           IF(gv_status=1)
           --
           THEN
           --
            FOR ln_pid IN pid_cursor
            --
            LOOP
               -- 
               --setting person_id in person_id variable(out variable)to insert person_id in further tables
               --
               p_person_id_o:=ln_pid.p_id;
               --
               --storing unique_id and person_id in client_mapping table
               --
               INSERT INTO client_mapping(unique_id
                                                ,person_id
                                                )
                                          VALUES(p_uniq_id_i
                                                ,p_person_id_o
                                                );
               --
            END LOOP;
            --
           END IF;
       EXCEPTION
       --       
          WHEN OTHERS
          --          
          THEN
          --
             dbms_output.put_line(sqlerrm);
             ROLLBACK;
             --
       END;
-----------------------------------------------------------------------------------------------------------------------
--                                                person_name definition
-----------------------------------------------------------------------------------------------------------------------
       --
       --this procedure validates the last_name if present it will store the record in person_name table 
       --
       PROCEDURE name_procedure(p_first_name_i        VARCHAR
                               ,p_middle_name_i       VARCHAR
                               ,p_last_name_i         VARCHAR
                               ,p_person_id_i         NUMBER
                               ,p_u_person_id_i       NUMBER
                              )
             IS
             --
       BEGIN
          --
          --checking whether last_name is null or not
          --
          IF(p_last_name_i IS NULL)
          --
          THEN
          --
             --
             --if null then store error message and object name
             --             
             gv_error_msg:=gv_error_msg||','||'last_name is mandatory';
             gv_object_name:=gv_object_name||','||'name';
          --
          END IF;
          --
          --if there is no error msg and person data is inserted for the record then store name in person_name
          --
          IF (gv_error_msg IS NULL)
          --
          THEN
          --
             --          
             --inserts the data if the data is not present in database
             --
             IF((p_u_person_id_i IS NULL)AND(gv_status=1))
             --
             THEN
          --
          --inserting name in person_name
          --
                INSERT INTO person_name(person_id
                                              ,first_name
                                              ,middle_name
                                              ,last_name
                                              )
                                        VALUES(p_person_id_i
                                              ,p_first_name_i
                                              ,p_middle_name_i
                                              ,p_last_name_i
                                              );
                --
                gv_status:=1;
                --
             --          
             --updates the data if the data is already present in database
             --   
             ELSIF((p_u_person_id_i IS NOT NULL)AND(gv_status=2))
             --
             THEN
             --
                UPDATE person_name
                   SET first_name=p_first_name_i
                      ,middle_name=p_middle_name_i
                      ,last_name=p_last_name_i
                 WHERE person_id=p_u_person_id_i;
                 --
                 gv_status:=2;
                 --
             --    
             END IF;
          --
          END IF;
          --
       EXCEPTION
       --
          WHEN OTHERS 
          --
          THEN
          --
             dbms_output.put_line(sqlerrm);
             --
       END;
-----------------------------------------------------------------------------------------------------------------------
--                                          address_ procedure definition
-----------------------------------------------------------------------------------------------------------------------
       PROCEDURE address_procedure(p_address_line_1_i   IN VARCHAR
                                  ,p_address_line_2_i   IN VARCHAR
                                  ,p_address_line_3_i   IN VARCHAR
                                  ,p_city_i             IN VARCHAR
                                  ,p_state_i            IN VARCHAR
                                  ,p_country_i          IN VARCHAR
                                  ,p_zip_code_i         IN VARCHAR
                                  ,p_person_id_i        IN NUMBER
                                  ,p_u_person_id_i      IN NUMBER
                                  ,p_address_id_i       IN NUMBER
                                 ) 
              IS
            --declaring address error flag
            ln_add_err NUMBER:=0;
            --
       BEGIN
       --
          --
          --checking whether address_line_1 is null or not
          --
          IF(p_address_line_1_i IS NULL)
          --
          THEN
          --  
             --
             --if error occurs then set address error flag to 1;
             --
              ln_add_err:=1;
              --
              --storing error msg
              --
             gv_error_msg:=gv_error_msg||','||'address_line_1 is mandatory';
          --
          ENd IF;
          --
          --checking whether state is null or not
          --
          IF(p_state_i IS NULL)
          --
          THEN
          --
             --
             --if error occurs then set address error flag to 1;
             --
             ln_add_err:=1;
             --
             --storing error msg
             --
             gv_error_msg:=gv_error_msg||','||'state is mandatory';
          --
          END IF;
          --
          --checking if there is any error and data is inserted in previous tables
          --
          IF((gv_error_msg IS NULL))
          --
          THEN
          --
             --          
             --inserts the data if the data is not there in database
             --
             IF((p_u_person_id_i IS NULL)AND(gv_status=1))
             --
             THEN
                --
                --storing address details in person_address table
                --
                INSERT INTO person_address(person_id
                                                 ,address_line_1
                                                 ,address_line_2
                                                 ,address_line_3
                                                 ,city
                                                 ,state
                                                 ,country
                                                 ,zip_code
                                                 )
                                           VALUES(p_person_id_i
                                                 ,p_address_line_1_i
                                                 ,p_address_line_2_i
                                                 ,p_address_line_3_i
                                                 ,p_city_i
                                                 ,p_state_i
                                                 ,p_country_i
                                                 ,p_zip_code_i
                                                 );
                --          
                --updates the data if the data is already present in database
                --
             ELSIF((p_u_person_id_i IS NOT NULL)AND(gv_status=2))
             --
             THEN
             --
                IF(p_address_id_i IS NULL)
                --
                THEN
                --
                   INSERT INTO person_address(person_id
                                                 ,address_line_1
                                                 ,address_line_2
                                                 ,address_line_3
                                                 ,city
                                                 ,state
                                                 ,country
                                                 ,zip_code
                                                 )
                                           VALUES(p_u_person_id_i
                                                 ,p_address_line_1_i
                                                 ,p_address_line_2_i
                                                 ,p_address_line_3_i
                                                 ,p_city_i
                                                 ,p_state_i
                                                 ,p_country_i
                                                 ,p_zip_code_i
                                                 );
               ELSE
               ---
             
                UPDATE person_address
                   SET address_line_1=p_address_line_1_i
                      ,address_line_2=p_address_line_2_i
                      ,address_line_3=p_address_line_3_i
                      ,city=p_city_i
                      ,state=p_state_i
                      ,country=p_country_i
                      ,zip_code=p_zip_code_i
                WHERE person_id=p_u_person_id_i;
                --
                END IF;
             --   
             END IF;
          END IF;
          --checking whether address error flag is set or not
          IF(ln_add_err=1)
          --
          THEN
          --
          --storing object name to identify the error occurred table name
          --
          gv_object_name:=gv_object_name||','||'address';
          --
          END IF;
          --
       EXCEPTION
       --       
            WHEN OTHERS
            --
            THEN
            --
               dbms_output.put_line(sqlerrm);
               ROLLBACK;
            --
       END;
-----------------------------------------------------------------------------------------------------------------------
--                                                error log definition
-----------------------------------------------------------------------------------------------------------------------
       --
       --This procedure stores the error details of the client record
       --
       PROCEDURE error_log(p_obj_name_i       VARCHAR
                          ,p_err_msg_i        VARCHAR
                          ,p_uniq_id_i        NUMBER
                          )
              IS
              --
           PRAGMA AUTONOMOUS_TRANSACTION;
           --
       BEGIN
       --
          --
          --storing error details
          --
          INSERT INTO error_log(object_name
                                      ,unique_id
                                      ,time
                                      ,error_message
                                      )
                                VALUES(p_obj_name_i
                                      ,p_uniq_id_i
                                      ,gt_timestamp
                                      ,p_err_msg_i
                                      );
          --
          COMMIT;
          --
       END;
        --
END data_simulation;
-----------------------------------------------------------------------------------------------------------------------
--                                                Execution statement
-----------------------------------------------------------------------------------------------------------------------
EXEC data_simulation.main;