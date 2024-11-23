import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

"""
EMF Query Example Output File

select prod, month, count(1.quant), count(2.quant)
from sales
where year=2020
group by prod, month
such that
1.prod=prod and 1.month=month+1 and 1.quant>avg(0.quant)
2.prod=prod and 2.month=month-1 and 2.quant>avg(0.quant)
having month > 1 and month < 12
"""

class H:
    prod=""
    month=0
    sum_0_quant=0
    count_0_quant=0
    avg_0_quant=0
    count_1_quant=0
    count_2_quant=0

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    mf_struct = []
    output_table = []
    mf_struct_dict = {}

    for row in cur:
        grouping_attrs_key = (row['prod'], row['month'])

        if row['year']==2020:
            if grouping_attrs_key not in mf_struct_dict:
                h = H()
                h.prod = row['prod']
                h.month = row['month']
                mf_struct.append(h)
                mf_struct_dict[grouping_attrs_key] = h

            h = mf_struct_dict[grouping_attrs_key]
            h.sum_0_quant += row['quant']
            h.count_0_quant += 1
            h.avg_0_quant = h.sum_0_quant/h.count_0_quant

    cur.scroll(0, mode='absolute')

    for row in cur:
        for h in mf_struct:
            if row['year']==2020:
                if row['prod']==h.prod and row['month']==h.month+1 and row['quant']>h.avg_0_quant:
                    h.count_1_quant += 1
    
    cur.scroll(0, mode='absolute')

    for row in cur:
        for h in mf_struct:
            if row['year']==2020:
                if row['prod']==h.prod and row['month']==h.month-1 and row['quant']>h.avg_0_quant:
                    h.count_2_quant += 1

    for h in mf_struct:
        if h.month>1 and h.month<12:
            output_table.append({'prod':h.prod, 'month':h.month, 'count_0_quant':h.count_0_quant, 'count_1_quant':h.count_1_quant, 'count_2_quant':h.count_2_quant})

    return tabulate.tabulate(output_table,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()