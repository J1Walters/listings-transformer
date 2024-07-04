import os
import shutil
import sqlean as sqlite3

sqlite3.extensions.enable('regexp')

OG_DB_PATH = 'C:/University/6G7V0007_MSC_Project/Project/Data/joblistings.db'
TRANSFORMED_DB_PATH = 'C:/University/6G7V0007_MSC_Project/Project/Data/joblistings_transformed.db'

def main():
    # Delete the tranformed database is already exists
    try:
        os.remove(TRANSFORMED_DB_PATH)
    except FileNotFoundError:
        pass

    # Create new database for transformed data
    con = sqlite3.connect(TRANSFORMED_DB_PATH)
    cur = con.cursor()
    cur.execute('CREATE TABLE website(id INTEGER PRIMARY KEY, name VARCHAR)')
    cur.execute('CREATE TABLE company(id INTEGER PRIMARY KEY, name VARCHAR)')
    cur.execute('''CREATE TABLE job(
        id INTEGER PRIMARY KEY,
        website_id INTEGER,
        company_id INTEGER,
        title VARCHAR,
        location VARCHAR,
        pay VARCHAR,
        description VARCHAR,
        timestamp VARCHAR,
        FOREIGN KEY (website_id) REFERENCES website(id),
        FOREIGN KEY (company_id) REFERENCES company(id)
        )'''
    )
    con.commit()
    con.close()

    # Open connection to original database and copy data
    con = sqlite3.connect(OG_DB_PATH)
    cur = con.cursor()
    cur.execute('ATTACH DATABASE ? AS new_db', (TRANSFORMED_DB_PATH,))
    cur.execute('INSERT INTO new_db.website SELECT * FROM website')
    cur.execute('INSERT INTO new_db.company SELECT * FROM company')
    cur.execute('INSERT INTO new_db.job SELECT * FROM job')
    con.commit()
    con.close()

    # Re-open connection to transformed database
    con = sqlite3.connect(TRANSFORMED_DB_PATH)
    cur = con.cursor()

    ### Transformations

    ## Clean company name and resolve duplicates
    print('Cleaning company table...')
    # Trim whitespace from entries
    cur.execute('UPDATE company SET name = TRIM(name)')

    # Create table containing id mappings for duplicates
    cur.execute('''CREATE TABLE company_id_map AS
                SELECT company.id AS old_id, min_id_tab.min_id AS new_id
                FROM 
                    (SELECT MIN(id) AS min_id, name 
                    FROM company 
                    GROUP BY name) min_id_tab
                INNER JOIN company
                ON min_id_tab.name = company.name'''
    )

    # Update company id in jobs table based on id mapping
    cur.execute('''UPDATE job
                SET company_id = id_map.new_id
                FROM
                    (SELECT company_id, new_id FROM job
                    INNER JOIN company_id_map
                    ON job.company_id = company_id_map.old_id
                    WHERE job.company_id <> company_id_map.new_id) id_map
                WHERE job.company_id = id_map.company_id
                '''
    )

    # # DEBUG
    # res = cur.execute('''SELECT company_id, new_id FROM job
    #                 INNER JOIN company_id_map
    #                 ON job.company_id = company_id_map.old_id
    #                 WHERE job.company_id <> company_id_map.new_id''')
    # for row in res:
    #     print(row)
    # res = cur.execute('SELECT * FROM job WHERE company_id NOT IN (SELECT MIN(id) FROM company GROUP BY name)')
    # for row in res:
    #     print(row)
    # res = cur.execute('SELECT * FROM job WHERE id = 1198')
    # for row in res:
    #     print(row)

    # Delete duplicated companies
    min_company_id = '''SELECT MIN(id)
                        FROM company 
                        GROUP BY name 
                        '''

    cur.execute(f'''DELETE FROM company
                WHERE id 
                NOT IN ({min_company_id})''')

    # # DEBUG
    # res = cur.execute('SELECT * FROM company')
    # for row in res:
    #     print(row)

    # Delete mapping table
    cur.execute('DROP TABLE company_id_map')

    ## Clean job title
    print('Cleaning job title...')
    clean_title_query = '''UPDATE job
                        SET title = REPLACE(REPLACE(title, "\t", ""), "\n", "") 
                        '''
    cur.execute(clean_title_query)

    # # DEBUG
    # res = cur.execute('SELECT title FROM job WHERE website_id = 2')
    # for row in res:
    #     print(row)

    ## Clean location
    print('Cleaning job location...')
    clean_location_query = '''UPDATE job
                            SET location = REPLACE(location, "Location ", "")
                            '''
    cur.execute(clean_location_query)

    # # DEBUG
    # res = cur.execute('SELECT location FROM job WHERE website_id = 2')
    # for row in res:
    #     print(row)

    ## Clean pay
    print('Cleaning job pay...')
    clean_pay_query = '''UPDATE job
                        SET pay = TRIM(REPLACE(REPLACE(pay, "Salary", ""), "+ benefits", ""))
                        '''
    cur.execute(clean_pay_query)

    # # DEBUG
    # res = cur.execute('SELECT pay FROM job')
    # for row in res:
    #     print(row)

    ## Clean job description
    print('Cleaning job description...')
    # Remove linebreaks and non-breaking spaces
    desc_replace_n = 'REPLACE(description, "\n", " ")'
    desc_replace_xa0 = f'REPLACE({desc_replace_n}, "\xa0", "")'
    # Remove Gradcracker pledge text
    desc_replace_gc_pledge = f'REPLACE({desc_replace_xa0}, \
        "We\'ve signed the Gradcracker feedback pledge. (This means that we will supply feedback if requested after an interview.)   1e127ede32d8f816eacfb0aed73cee11", \
        "")'
    # Remove left over text from video controls
    desc_replace_pauseplay = f'REGEXP_REPLACE({desc_replace_gc_pledge}, "PausePlay.*fullscreen", "")'
    # Strip whitespace
    desc_strip_whitespace = f'TRIM({desc_replace_pauseplay})'
    # Remove excess whitespaces from inside description
    desc_replace_whitespace = f'REGEXP_REPLACE({desc_strip_whitespace}, "\\s\\s+", " ")'
    # Run query
    clean_desc_query = f'''UPDATE job
                        SET description = {desc_replace_whitespace}
                        '''
    cur.execute(clean_desc_query)

    # # DEBUG
    # res = cur.execute('SELECT description FROM job')
    # for row in res:
    #     print(row)

    ## Check for duplicates
    print('Removing duplicate jobs...')

    min_listing_id = '''SELECT MIN(id)
                        FROM job 
                        GROUP BY company_id, title, location, pay, description
                        '''

    # # DEBUG
    # res = cur.execute(min_listing_id)
    # for row in res:
    #     print(row)

    duplicates = f'''SELECT id
                    FROM job 
                    WHERE id NOT IN ({min_listing_id})
                    '''

    # # DEBUG
    # res = cur.execute(duplicates)
    # for row in res:
    #     print(row)

    res = cur.execute(f'SELECT COUNT(*) FROM ({duplicates})')
    num_duplicates = res.fetchone()[0]

    print(f'Number of duplicates: {num_duplicates}')

    # Remove duplicates
    remove_duplicates = f'''DELETE FROM job
                        WHERE id IN ({duplicates})
                        '''

    cur.execute(remove_duplicates)

    ## Print number of remaining entries
    print('Finished.')

    res = cur.execute('SELECT COUNT(*) FROM job')
    num_entries = res.fetchone()[0]

    print(f'Remaining Entries: {num_entries}')

    ## Commit changes
    con.commit()

    ## Close connection
    con.close()

if __name__ == '__main__':
    main()
