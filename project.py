import psycopg2

def numberOne():
    conn = psycopg2.connect("dbname=news")
    c = conn.cursor()
    query = '''
    SELECT articles.title , count(*) as views FROM articles, log 
    WHERE log.path = CONCAT('/article/', articles.slug) 
    GROUP BY articles.title
    ORDER BY views DESC LIMIT 3;
    '''
    c.execute(query)
    rows = c.fetchall()

    for row in rows:
        print(row)
    conn.close()


def numberTwo():
    conn = psycopg2.connect("dbname=news")
    c = conn.cursor()
    query = '''
    SELECT authors.name, count(*) as views FROM authors, log, articles 
    WHERE articles.author = authors.id AND log.path = CONCAT('/article/', articles.slug)
    GROUP BY authors.name 
    ORDER BY views DESC; 
    '''
    c.execute(query)
    rows = c.fetchall()
    for row in rows:
        print(row)
    conn.close()

   
def numberThree():
    conn = psycopg2.connect("dbname=news")
    c = conn.cursor()
    query = '''
    create view error_status as select date(time) as date count(*) as percent from log where status !='200 ok'
    group by date limit 500;


    create view all_errors as 
    select date(time) as date , count(*) as percent from log 
    group by date;


    create view percentage as 
    select a.date, (b.percent * 100 / a.percent) as error 
    from error_status as b , all_errors as a 
    where a.date = b.date;
    '''
    c.execute(query)
    rows = c.fetchall()
    for row in rows:
        print(row)
    conn.close()

      

if __name__=='__main__':
    numberOne()
    numberThree()
    numberTwo()
