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

    # Copy the original database
    shutil.copyfile(OG_DB_PATH, TRANSFORMED_DB_PATH)

    # Open connection to database and define cursor
    con = sqlite3.connect(TRANSFORMED_DB_PATH)
    cur = con.cursor()
    
    # Transformations
    
    # Clean company name
    res = cur.execute('SELECT RTRIM(name) FROM company')
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
    



if __name__ == '__main__':
    main()
