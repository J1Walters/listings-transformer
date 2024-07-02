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
    
    # Transformations
    
    # Clean company name
    # Trim whitespace from entries
    cur.execute('UPDATE company SET name = TRIM(name)')
    
    # res = cur.execute('SELECT * FROM company GROUP BY name HAVING COUNT(*) > 1')
    # for row in res:
    #     print(row)
    min_company_id = 'SELECT MIN(id) FROM company GROUP BY name ORDER BY MIN(id)'
    
    # res = cur.execute(f'SELECT * FROM job WHERE company_id NOT IN ({min_company_id})')
    # res = cur.execute(test)
    # for row in res:
    #     print(row)
    
    # Create table containing id mappings for duplicatess
    cur.execute('''CREATE TABLE company_id_map AS
                SELECT tab2.id AS old_id, tab1.min_id AS new_id
                FROM (SELECT MIN(id) AS min_id, name FROM company GROUP BY name) tab1
                INNER JOIN company tab2 
                ON tab1.name = tab2.name'''
    )
    
    res = cur.execute('SELECT * FROM company_id_map')
    for row in res:
        print(row)
    
    # Test deletion
    cur.execute(f'DELETE FROM company WHERE id NOT IN ({min_company_id})')
    res = cur.execute('SELECT * FROM company')
    # for row in res:
    #     print(row)
    
    # Clean job title
    clean_title_query = 'SELECT REPLACE(REPLACE(title, "\t", ""), "\n", "") FROM job'
    res = cur.execute(clean_title_query)
    # for row in res:
    #     print(row)
    
    # Clean location
    clean_location_query = 'SELECT REPLACE(location, "Location ", "") FROM job'
    res = cur.execute(clean_location_query)
    # for row in res:
    #     print(row)
    
    # Clean pay
    clean_pay_query = 'SELECT REPLACE(REPLACE(pay, "Salary ", ""), "   + benefits ", "") FROM job'
    res = cur.execute(clean_pay_query)
    # for row in res:
    #     print(row)

    # Clean job description
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
    # Run query
    clean_desc_query = f'SELECT {desc_strip_whitespace} FROM job LIMIT 10'
    res = cur.execute(clean_desc_query)
    # for row in res:
    #     print(row)
    
    # Check for duplicates
    dupe_query = 'SELECT COUNT(*) AS count, website_id, company_id, title, location, pay, description FROM job GROUP BY company_id, title, location, pay, description HAVING COUNT(*) > 1'
    res = cur.execute(dupe_query)
    # for row in res:
    #     print(row)
        
    test_query = 'SELECT * FROM job WHERE company_id = 14'
    res = cur.execute(test_query)
    # for row in res:
    #     print(row)



if __name__ == '__main__':
    main()
